"""
Gold Price Consumer — With VWAP + MACD
========================================
- بيقرأ ticks من Redpanda
- بيحسب RSI / EMA / Bollinger / VWAP / MACD
- بيكتب النتايج في QuestDB

Install:
    pip install confluent-kafka polars questdb
"""

import json
import signal
import socket
import time
from collections import deque
from datetime import datetime, timezone

import polars as pl
from confluent_kafka import Consumer, KafkaError


# ─────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────
REDPANDA_BROKER  = "localhost:19092"
TOPIC            = "gold-price-ticks"
GROUP_ID         = "gold-consumer-group"
QUESTDB_HOST     = "localhost"
QUESTDB_ILP_PORT = 9009
TABLE_NAME       = "gold_ticks"
MIN_TICKS        = 26   # MACD محتاج EMA26 كأطول period



class QuestDBWriter:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.sock = None
        self._connect()

    def _connect(self):
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect((self.host, self.port))
            print(f"[QuestDB] Connected → {self.host}:{self.port}")
        except Exception as e:
            print(f"[QuestDB] Connection failed: {e}")
            self.sock = None

    def write(self, row: dict):
        if not self.sock:
            self._connect()
            if not self.sock:
                return

        ts_ns = int(datetime.now(timezone.utc).timestamp() * 1_000_000_000)

        line = (
            f"{TABLE_NAME},"
            f"symbol={row['symbol']} "
            f"price={row['price']},"
            f"bid={row.get('bid', row['price'])},"
            f"ask={row.get('ask', row['price'])},"
            f"volume={row.get('volume', 1)},"
            f"ema_9={row.get('ema_9', 0.0)},"
            f"ema_21={row.get('ema_21', 0.0)},"
            f"rsi_14={row.get('rsi_14', 50.0)},"
            f"bb_upper={row.get('bb_upper', 0.0)},"
            f"bb_middle={row.get('bb_middle', 0.0)},"
            f"bb_lower={row.get('bb_lower', 0.0)},"
            f"bb_pct_b={row.get('bb_pct_b', 0.5)},"
            f"vwap={row.get('vwap', 0.0)},"
            f"macd_line={row.get('macd_line', 0.0)},"
            f"macd_signal={row.get('macd_signal', 0.0)},"
            f"macd_hist={row.get('macd_hist', 0.0)},"
            f"signal=\"{row.get('signal', 'HOLD')}\""
            f" {ts_ns}\n"
        )

        try:
            self.sock.sendall(line.encode("utf-8"))
        except Exception as e:
            print(f"[QuestDB] Write error: {e} — reconnecting...")
            self.sock = None

    def close(self):
        if self.sock:
            self.sock.close()



class IndicatorsEngine:
    def __init__(self, maxlen: int = 300):
        self.prices:  deque = deque(maxlen=maxlen)
        self.volumes: deque = deque(maxlen=maxlen)

        self._cum_pv: float = 0.0
        self._cum_v:  float = 0.0

        self._ema12: float = None
        self._ema26: float = None
        self._macd_signal: float = None

        self._prev_ema_rel: bool = None

    def update(self, price: float, volume: float = 1.0) -> dict:
        self.prices.append(price)
        self.volumes.append(volume)
        n = len(self.prices)

        self._cum_pv += price * volume
        self._cum_v  += volume

        result = {
            "price":       price,
            "volume":      volume,
            "ema_9":       0.0,
            "ema_21":      0.0,
            "rsi_14":      50.0,
            "bb_upper":    price,
            "bb_middle":   price,
            "bb_lower":    price,
            "bb_pct_b":    0.5,
            "vwap":        0.0,
            "macd_line":   0.0,
            "macd_signal": 0.0,
            "macd_hist":   0.0,
            "signal":      f"WAIT ({n}/{MIN_TICKS})",
        }

        if self._cum_v > 0:
            result["vwap"] = round(self._cum_pv / self._cum_v, 4)

        if n < MIN_TICKS:
            return result

        prices_list = list(self.prices)

        result["ema_9"]  = self._calc_ema(prices_list, 9)
        result["ema_21"] = self._calc_ema(prices_list, 21)

        result["rsi_14"] = self._calc_rsi(prices_list, 14)

        result.update(self._calc_bollinger(prices_list, price))

        result.update(self._calc_macd(price))

        result["signal"] = self._generate_signal(result)

        return result


    def _calc_ema(self, prices: list, period: int) -> float:
        k   = 2 / (period + 1)
        ema = sum(prices[:period]) / period
        for p in prices[period:]:
            ema = p * k + ema * (1 - k)
        return round(ema, 4)

    def _calc_rsi(self, prices: list, period: int = 14) -> float:
        deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
        gains  = [max(d, 0)   for d in deltas]
        losses = [abs(min(d, 0)) for d in deltas]
        ag = sum(gains[:period])  / period
        al = sum(losses[:period]) / period
        for i in range(period, len(deltas)):
            ag = (ag * (period - 1) + gains[i])  / period
            al = (al * (period - 1) + losses[i]) / period
        if al == 0:
            return 100.0
        return round(100 - (100 / (1 + ag / al)), 2)

    def _calc_bollinger(self, prices: list, price: float, period: int = 20) -> dict:
        if len(prices) < period:
            return {"bb_upper": price, "bb_middle": price,
                    "bb_lower": price, "bb_pct_b": 0.5}
        window = prices[-period:]
        middle = sum(window) / period
        std    = (sum((p - middle) ** 2 for p in window) / period) ** 0.5
        upper  = round(middle + 2 * std, 4)
        lower  = round(middle - 2 * std, 4)
        middle = round(middle, 4)
        pct_b  = (price - lower) / (upper - lower) if upper != lower else 0.5
        return {
            "bb_upper":  upper,
            "bb_middle": middle,
            "bb_lower":  lower,
            "bb_pct_b":  round(pct_b, 4),
        }

    def _calc_macd(self, price: float) -> dict:
        """
        MACD = EMA12 - EMA26
        Signal = EMA9 of MACD
        Histogram = MACD - Signal
        """
        k12 = 2 / (12 + 1)
        k26 = 2 / (26 + 1)
        k9  = 2 / (9  + 1)

        if self._ema12 is None:
            self._ema12 = price
            self._ema26 = price
        else:
            self._ema12 = price * k12 + self._ema12 * (1 - k12)
            self._ema26 = price * k26 + self._ema26 * (1 - k26)

        macd_line = round(self._ema12 - self._ema26, 4)

        if self._macd_signal is None:
            self._macd_signal = macd_line
        else:
            self._macd_signal = (
                macd_line * k9 + self._macd_signal * (1 - k9)
            )

        macd_signal = round(self._macd_signal, 4)
        macd_hist   = round(macd_line - macd_signal, 4)

        return {
            "macd_line":   macd_line,
            "macd_signal": macd_signal,
            "macd_hist":   macd_hist,
        }

    def _generate_signal(self, r: dict) -> str:
        rsi       = r["rsi_14"]
        pct_b     = r["bb_pct_b"]
        ema9      = r["ema_9"]
        ema21     = r["ema_21"]
        macd_hist = r["macd_hist"]
        price     = r["price"]
        vwap      = r["vwap"]

        if rsi < 30 and pct_b < 0.2 and macd_hist > 0:
            return "STRONG BUY 🟢🟢"

        if rsi > 70 and pct_b > 0.8 and macd_hist < 0:
            return "STRONG SELL 🔴🔴"

        if rsi < 35 and macd_hist > 0:
            return "BUY 🟢"

        if rsi > 65 and macd_hist < 0:
            return "SELL 🔴"

        if ema9 > ema21:
            return "BULLISH ↑"
        if ema9 < ema21:
            return "BEARISH ↓"

        return "HOLD ⚪"



def run_consumer():
    print("=" * 55)
    print("  ⚙️  Gold Consumer — VWAP + MACD Edition")
    print(f"  Broker : {REDPANDA_BROKER}")
    print(f"  Topic  : {TOPIC}")
    print(f"  DB     : {QUESTDB_HOST}:{QUESTDB_ILP_PORT}")
    print("=" * 55)

    consumer = Consumer({
        "bootstrap.servers":  REDPANDA_BROKER,
        "group.id":           GROUP_ID,
        "auto.offset.reset":  "latest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([TOPIC])

    db     = QuestDBWriter(QUESTDB_HOST, QUESTDB_ILP_PORT)
    engine = IndicatorsEngine(maxlen=300)

    running = True
    def shutdown(sig, frame):
        nonlocal running
        print("\n[INFO] Shutting down consumer...")
        running = False
    signal.signal(signal.SIGINT, shutdown)

    tick_count = 0
    start_time = time.time()

    while running:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            print(f"[ERROR] Kafka: {msg.error()}")
            continue

        try:
            tick   = json.loads(msg.value().decode("utf-8"))
            price  = float(tick["price"])
            volume = float(tick.get("volume", 1.0))

            indicators = engine.update(price, volume)

            row = {
                "symbol": tick.get("symbol", "XAU/USD"),
                "price":  price,
                "bid":    tick.get("bid", price),
                "ask":    tick.get("ask", price),
                **indicators,
            }

            db.write(row)
            tick_count += 1

            if tick_count % 10 == 0:
                print(
                    f"[{datetime.now().strftime('%H:%M:%S')}] "
                    f"Price=${price:,.2f} | "
                    f"RSI={indicators['rsi_14']:.1f} | "
                    f"MACD={indicators['macd_line']:.3f} | "
                    f"VWAP=${indicators['vwap']:,.2f} | "
                    f"Signal={indicators['signal']}"
                )

        except Exception as e:
            print(f"[ERROR] {e}")

    consumer.close()
    db.close()
    print(f"[INFO] Done. Processed {tick_count} ticks.")


if __name__ == "__main__":
    run_consumer()