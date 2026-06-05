"""
Gold Price Ingestor — Twelve Data WebSocket
============================================
- بيتصل بـ Twelve Data WebSocket API
- بيستقبل أسعار XAU/USD real-time
- بيبعت كل tick لـ Redpanda

Install:
    pip install confluent-kafka websockets python-dotenv
"""

import asyncio
import json
import signal
import time
from datetime import datetime, timezone
import websockets
from confluent_kafka import Producer

API_KEY         = "0c69d44732ed4bf696540292d8e19b36"
WS_URL          = f"wss://ws.twelvedata.com/v1/quotes/price?apikey={API_KEY}"
SYMBOL          = "XAU/USD"
REDPANDA_BROKER = "localhost:19092"
TOPIC           = "gold-price-ticks"


def create_producer() -> Producer:
    return Producer({
        "bootstrap.servers": REDPANDA_BROKER,
        "client.id":         "gold-ingestor-live",
        "acks":              "all",
        "retries":           5,
        "retry.backoff.ms":  500,
    })


def delivery_report(err, msg):
    if err:
        print(f"[ERROR] Delivery failed: {err}")


# ─────────────────────────────────────────
#  WEBSOCKET INGESTOR
# ─────────────────────────────────────────
async def run_ingestor():
    print("=" * 55)
    print("  🟡 Gold Ingestor — Twelve Data WebSocket")
    print(f"  Symbol : {SYMBOL}")
    print(f"  Broker : {REDPANDA_BROKER}")
    print(f"  Topic  : {TOPIC}")
    print("=" * 55)

    producer   = create_producer()
    tick_count = 0
    start_time = time.time()

    running = True
    def shutdown(sig, frame):
        nonlocal running
        print("\n[INFO] Shutting down...")
        running = False
    signal.signal(signal.SIGINT, shutdown)

    while running:
        try:
            print("[INFO] Connecting to Twelve Data WebSocket...")

            async with websockets.connect(WS_URL) as ws:
                print("[INFO] Connected ✅")

                # Subscribe لـ XAU/USD
                subscribe_msg = json.dumps({
                    "action": "subscribe",
                    "params": {
                        "symbols": SYMBOL
                    }
                })
                await ws.send(subscribe_msg)
                print(f"[INFO] Subscribed to {SYMBOL}")

                async for raw_msg in ws:
                    if not running:
                        break

                    try:
                        data = json.loads(raw_msg)

                        event = data.get("event", "")

                        if event in ("heartbeat", "subscribe-status", ""):
                            if event == "heartbeat":
                                print("[♥] heartbeat")
                            continue

                        if event == "price" or "price" in data:
                            price = float(data.get("price", 0))

                            if price <= 0:
                                continue

                            tick = {
                                "symbol":    data.get("symbol", SYMBOL),
                                "price":     price,
                                "bid":       float(data.get("bid", price)),
                                "ask":       float(data.get("ask", price)),
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                                "tick":      tick_count + 1,
                                "source":    "twelvedata",
                            }

                            payload = json.dumps(tick).encode("utf-8")
                            producer.produce(
                                topic    = TOPIC,
                                value    = payload,
                                key      = SYMBOL.encode("utf-8"),
                                callback = delivery_report,
                            )
                            producer.poll(0)

                            tick_count += 1

                            elapsed = time.time() - start_time
                            print(
                                f"[{tick['timestamp'][11:19]}] "
                                f"Price=${price:,.2f} | "
                                f"Ticks={tick_count} | "
                                f"Uptime={elapsed:.0f}s"
                            )

                    except (json.JSONDecodeError, KeyError, ValueError) as e:
                        print(f"[WARN] Parse error: {e} — raw: {raw_msg[:100]}")

        except websockets.exceptions.ConnectionClosed as e:
            print(f"[WARN] WebSocket closed: {e} — reconnecting in 5s...")
            await asyncio.sleep(5)

        except Exception as e:
            print(f"[ERROR] {e} — reconnecting in 10s...")
            await asyncio.sleep(10)

    producer.flush(5)
    print(f"[INFO] Done. Sent {tick_count} ticks total.")


if __name__ == "__main__":
    asyncio.run(run_ingestor())