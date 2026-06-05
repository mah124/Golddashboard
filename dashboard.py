"""
Gold Price Real-Time Dashboard — VWAP + MACD + Alerts
======================================================
Run:
    streamlit run dashboard.py
"""

from datetime import datetime
from collections import deque

import plotly.graph_objects as go
from plotly.subplots import make_subplots
import psycopg2
import polars as pl
import streamlit as st


# ─────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────
import streamlit as st

QUESTDB_HOST = st.secrets.get("QUESTDB_HOST", "localhost")
QUESTDB_PORT = int(st.secrets.get("QUESTDB_PORT", 8812))
QUESTDB_DB   = "qdb"
QUESTDB_USER = st.secrets.get("QUESTDB_USER", "admin")
QUESTDB_PASS = st.secrets.get("QUESTDB_PASS", "quest")
TABLE        = "gold_ticks"
REFRESH_SEC  = 2
LOOKBACK     = 200


# ─────────────────────────────────────────
#  PAGE SETUP
# ─────────────────────────────────────────
st.set_page_config(
    page_title="🟡 Gold Dashboard",
    page_icon="🟡",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
    .block-container { padding-top: 1rem; }
    .signal-buy    { color: #00e676; font-size: 1.3rem; font-weight: bold; }
    .signal-sell   { color: #ff5252; font-size: 1.3rem; font-weight: bold; }
    .signal-hold   { color: #bdbdbd; font-size: 1.3rem; font-weight: bold; }
    .signal-wait   { color: #ffab40; font-size: 1.3rem; font-weight: bold; }
    .metric-card   { background:#1a1a2e; border-radius:12px; padding:14px 18px; border:1px solid #2d2d4e; }
    .alert-buy     { background:rgba(0,230,118,0.1); border-left:4px solid #00e676; padding:7px 12px; border-radius:5px; margin:3px 0; font-size:0.82rem; }
    .alert-sell    { background:rgba(255,82,82,0.1);  border-left:4px solid #ff5252; padding:7px 12px; border-radius:5px; margin:3px 0; font-size:0.82rem; }
    .alert-spike   { background:rgba(255,171,64,0.1); border-left:4px solid #ffab40; padding:7px 12px; border-radius:5px; margin:3px 0; font-size:0.82rem; }
    .alert-cross   { background:rgba(100,181,246,0.1);border-left:4px solid #64b5f6; padding:7px 12px; border-radius:5px; margin:3px 0; font-size:0.82rem; }
    .section-title { font-size:0.85rem; color:#aaa; margin-bottom:4px; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────
#  SESSION STATE
# ─────────────────────────────────────────
for key, default in [
    ("alert_log",    deque(maxlen=50)),
    ("prev_signal",  ""),
    ("prev_ema_rel", None),
    ("prev_price",   None),
    ("prev_display", None),
]:
    if key not in st.session_state:
        st.session_state[key] = default


# ─────────────────────────────────────────
#  QUESTDB
# ─────────────────────────────────────────
@st.cache_resource
def get_connection():
    try:
        conn = psycopg2.connect(
            host=QUESTDB_HOST, port=QUESTDB_PORT,
            dbname=QUESTDB_DB, user=QUESTDB_USER, password=QUESTDB_PASS,
        )
        conn.autocommit = True
        return conn
    except Exception as e:
        st.error(f"QuestDB connection failed: {e}")
        return None


def fetch_data(conn) -> pl.DataFrame:
    query = f"""
        SELECT timestamp, price, bid, ask,
               ema_9, ema_21, rsi_14,
               bb_upper, bb_middle, bb_lower, bb_pct_b,
               vwap, macd_line, macd_signal, macd_hist,
               signal
        FROM {TABLE}
        ORDER BY timestamp DESC
        LIMIT {LOOKBACK}
    """
    try:
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        cur.close()
        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(rows, schema=cols, orient="row").sort("timestamp")
    except Exception as e:
        st.error(f"Query error: {e}")
        return pl.DataFrame()


# ─────────────────────────────────────────
#  ALERT ENGINE
# ─────────────────────────────────────────
def check_alerts(price, rsi, pct_b, ema9, ema21, macd_hist):
    now    = datetime.now().strftime("%H:%M:%S")
    alerts = []

    if rsi < 30 and pct_b < 0.2:
        alerts.append({"type":"buy",   "css":"alert-buy",   "icon":"🟢",
            "msg": f"BUY — RSI={rsi:.1f} | %B={pct_b:.2f} | ${price:,.2f}"})

    if rsi > 70 and pct_b > 0.8:
        alerts.append({"type":"sell",  "css":"alert-sell",  "icon":"🔴",
            "msg": f"SELL — RSI={rsi:.1f} | %B={pct_b:.2f} | ${price:,.2f}"})

    if st.session_state.prev_price:
        delta = abs(price - st.session_state.prev_price)
        if delta >= 2.0:
            d = "↑" if price > st.session_state.prev_price else "↓"
            alerts.append({"type":"spike","css":"alert-spike","icon":"⚡",
                "msg": f"Spike {d} ${delta:.2f} → ${price:,.2f}"})

    ema_rel = ema9 > ema21
    if st.session_state.prev_ema_rel is not None:
        if ema_rel and not st.session_state.prev_ema_rel:
            alerts.append({"type":"cross","css":"alert-cross","icon":"📈",
                "msg": f"EMA Cross UP — EMA9={ema9:.2f} > EMA21={ema21:.2f}"})
        elif not ema_rel and st.session_state.prev_ema_rel:
            alerts.append({"type":"cross","css":"alert-cross","icon":"📉",
                "msg": f"EMA Cross DOWN — EMA9={ema9:.2f} < EMA21={ema21:.2f}"})

    if macd_hist > 0 and (st.session_state.prev_price or 0) < price:
        pass  # MACD confirmation — يمكن تضيفه لاحقًا

    st.session_state.prev_price   = price
    st.session_state.prev_ema_rel = ema_rel

    for a in alerts:
        a["time"] = now
        st.session_state.alert_log.appendleft(a)

    return alerts


# ─────────────────────────────────────────
#  CHARTS
# ─────────────────────────────────────────
GOLD   = "#FFD700"
EMA9C  = "#00BCD4"
EMA21C = "#FF7043"
VWAPC  = "#AB47BC"
BB_COL = "rgba(100,100,200,0.12)"
DARK   = "rgba(15,15,30,0.85)"


def build_main_chart(df: pl.DataFrame) -> go.Figure:
    ts     = df["timestamp"].to_list()
    price  = df["price"].to_list()
    ema9   = df["ema_9"].to_list()
    ema21  = df["ema_21"].to_list()
    bb_up  = df["bb_upper"].to_list()
    bb_mid = df["bb_middle"].to_list()
    bb_low = df["bb_lower"].to_list()
    vwap   = df["vwap"].to_list()

    fig = go.Figure()

    # BB fill
    fig.add_trace(go.Scatter(
        x=ts + ts[::-1], y=bb_up + bb_low[::-1],
        fill="toself", fillcolor=BB_COL,
        line=dict(color="rgba(0,0,0,0)"),
        name="Bollinger Band", hoverinfo="skip",
    ))
    for y, dash in [(bb_up, "dot"), (bb_low, "dot")]:
        fig.add_trace(go.Scatter(x=ts, y=y,
            line=dict(color="rgba(100,100,200,0.4)", width=1, dash=dash),
            showlegend=False, hoverinfo="skip"))
    fig.add_trace(go.Scatter(x=ts, y=bb_mid,
        line=dict(color="rgba(100,100,200,0.25)", width=1),
        showlegend=False, hoverinfo="skip"))

    # VWAP
    fig.add_trace(go.Scatter(x=ts, y=vwap,
        line=dict(color=VWAPC, width=1.5, dash="dash"),
        name="VWAP"))

    # EMAs
    fig.add_trace(go.Scatter(x=ts, y=ema9,
        line=dict(color=EMA9C, width=1.5), name="EMA 9"))
    fig.add_trace(go.Scatter(x=ts, y=ema21,
        line=dict(color=EMA21C, width=1.5), name="EMA 21"))

    # Price
    fig.add_trace(go.Scatter(x=ts, y=price,
        line=dict(color=GOLD, width=2.5), name="XAU/USD"))

    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor=DARK,
        font=dict(color="#e0e0e0"),
        margin=dict(l=8, r=8, t=8, b=8),
        legend=dict(orientation="h", y=1.06, x=0, bgcolor="rgba(0,0,0,0)", font=dict(size=11)),
        xaxis=dict(gridcolor="rgba(255,255,255,0.04)", showgrid=True),
        yaxis=dict(gridcolor="rgba(255,255,255,0.04)", tickformat=".2f"),
        hovermode="x unified", height=360,
    )
    return fig


def build_rsi_chart(df: pl.DataFrame) -> go.Figure:
    ts  = df["timestamp"].to_list()
    rsi = df["rsi_14"].to_list()

    fig = go.Figure()
    fig.add_hrect(y0=70, y1=100, fillcolor="rgba(255,82,82,0.08)",  line_width=0)
    fig.add_hrect(y0=0,  y1=30,  fillcolor="rgba(0,230,118,0.08)",  line_width=0)
    fig.add_hline(y=70, line=dict(color="rgba(255,82,82,0.5)",  dash="dash", width=1))
    fig.add_hline(y=30, line=dict(color="rgba(0,230,118,0.5)",  dash="dash", width=1))
    fig.add_hline(y=50, line=dict(color="rgba(255,255,255,0.15)", dash="dot", width=1))

    fig.add_trace(go.Scatter(x=ts, y=rsi,
        line=dict(color="#CE93D8", width=2),
        fill="tozeroy", fillcolor="rgba(206,147,216,0.08)",
        name="RSI 14"))

    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor=DARK,
        font=dict(color="#e0e0e0"),
        margin=dict(l=8, r=8, t=4, b=4),
        yaxis=dict(range=[0, 100], gridcolor="rgba(255,255,255,0.04)"),
        xaxis=dict(gridcolor="rgba(255,255,255,0.04)"),
        showlegend=False, height=150,
    )
    return fig


def build_macd_chart(df: pl.DataFrame) -> go.Figure:
    filtered = df.filter(
        pl.col("macd_hist").is_not_null() &
        pl.col("macd_line").is_not_null() &
        pl.col("macd_signal").is_not_null()
    )
    fig = go.Figure()
    if filtered.is_empty():
        fig.add_annotation(text="Waiting for MACD data...",
            xref="paper", yref="paper", x=0.5, y=0.5,
            showarrow=False, font=dict(color="#888", size=12))
        fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor=DARK,
            margin=dict(l=8,r=8,t=4,b=4), height=150)
        return fig
    ts   = filtered["timestamp"].to_list()
    macd = filtered["macd_line"].to_list()
    sig  = filtered["macd_signal"].to_list()
    hist = filtered["macd_hist"].to_list()
    colors = ["rgba(0,230,118,0.7)" if (h or 0) >= 0 else "rgba(255,82,82,0.7)" for h in hist]
    fig.add_trace(go.Bar(x=ts, y=hist, marker_color=colors, name="Histogram", opacity=0.8))
    fig.add_trace(go.Scatter(x=ts, y=macd, line=dict(color="#00BCD4", width=1.5), name="MACD"))
    fig.add_trace(go.Scatter(x=ts, y=sig,  line=dict(color="#FF7043", width=1.5), name="Signal"))
    fig.add_hline(y=0, line=dict(color="rgba(255,255,255,0.2)", width=1))
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor=DARK,
        font=dict(color="#e0e0e0"),
        margin=dict(l=8, r=8, t=4, b=4),
        legend=dict(orientation="h", y=1.1, x=0, bgcolor="rgba(0,0,0,0)", font=dict(size=10)),
        xaxis=dict(gridcolor="rgba(255,255,255,0.04)"),
        yaxis=dict(gridcolor="rgba(255,255,255,0.04)", tickformat=".3f"),
        barmode="relative", height=150,
    )
    return fig


def signal_class(s: str) -> str:
    u = s.upper()
    if "BUY"  in u: return "signal-buy"
    if "SELL" in u: return "signal-sell"
    if "WAIT" in u: return "signal-wait"
    return "signal-hold"


# ─────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────
def main():
    st.markdown("## 🟡 Gold Dashboard — XAU/USD")
    st.caption(f"Real-time · Refresh {REFRESH_SEC}s · VWAP + MACD + RSI + Bollinger")

    conn = get_connection()
    if not conn:
        st.stop()

    @st.fragment(run_every=REFRESH_SEC)
    def live_dashboard():
        df = fetch_data(conn)
        if df.is_empty():
            st.warning("⏳ Waiting for data...")
            return

        latest    = df.tail(1)
        price     = float(latest["price"][0])
        ema9      = float(latest["ema_9"][0])
        ema21     = float(latest["ema_21"][0])
        rsi       = float(latest["rsi_14"][0])
        bb_up     = float(latest["bb_upper"][0])
        bb_low    = float(latest["bb_lower"][0])
        pct_b     = float(latest["bb_pct_b"][0])
        vwap      = float(latest["vwap"][0])
        macd_line = float(latest["macd_line"][0])
        macd_sig  = float(latest["macd_signal"][0])
        macd_hist = float(latest["macd_hist"][0])
        signal    = str(latest["signal"][0])

        # Alerts
        new_alerts = check_alerts(price, rsi, pct_b, ema9, ema21, macd_hist)
        for a in new_alerts:
            if   a["type"] == "buy":   st.success(f"{a['icon']} {a['msg']}")
            elif a["type"] == "sell":  st.error  (f"{a['icon']} {a['msg']}")
            elif a["type"] == "spike": st.warning(f"{a['icon']} {a['msg']}")
            elif a["type"] == "cross": st.info   (f"{a['icon']} {a['msg']}")

        # ── KPIs Row ─────────────────────────────────
        c1,c2,c3,c4,c5,c6,c7 = st.columns(7)

        prev = st.session_state.get("prev_display") or price
        c1.metric("💰 XAU/USD", f"${price:,.2f}",   delta=f"{price-prev:+.2f}")
        c2.metric("EMA 9",      f"${ema9:,.2f}",    delta=f"{price-ema9:+.2f}")
        c3.metric("EMA 21",     f"${ema21:,.2f}",   delta=f"{price-ema21:+.2f}")
        c4.metric("VWAP",       f"${vwap:,.2f}",    delta=f"{price-vwap:+.2f}")
        c5.metric("RSI 14",     f"{rsi:.1f}",
                  delta="🔴 OB" if rsi > 70 else ("🟢 OS" if rsi < 30 else "⚪"))
        c6.metric("MACD",       f"{macd_line:.3f}", delta=f"H:{macd_hist:+.3f}")

        css = signal_class(signal)
        c7.markdown(
            f'<div class="metric-card">'
            f'<div class="section-title">Signal</div>'
            f'<div class="{css}">{signal}</div>'
            f'</div>', unsafe_allow_html=True)

        st.session_state["prev_display"] = price

        st.divider()

        # ── Charts + Alerts ───────────────────────────
        col_ch, col_al = st.columns([3, 1])

        with col_ch:
            # Price chart
            st.plotly_chart(build_main_chart(df),
                width='stretch', config={"displayModeBar": False})

            # RSI + MACD side by side
            r1, r2 = st.columns(2)
            with r1:
                st.caption("📊 RSI 14")
                st.plotly_chart(build_rsi_chart(df),
                    width='stretch', config={"displayModeBar": False})
            with r2:
                st.caption("📈 MACD (12, 26, 9)")
                st.plotly_chart(build_macd_chart(df),
                    width='stretch', config={"displayModeBar": False})

        with col_al:
            prices = df["price"].to_list()
            st.caption("📊 Session Stats")
            st.markdown(f"""
| | |
|---|---|
| **High** | ${max(prices):,.2f} |
| **Low** | ${min(prices):,.2f} |
| **Range** | ${max(prices)-min(prices):,.2f} |
| **VWAP** | ${vwap:,.2f} |
| **BB Upper** | ${bb_up:,.2f} |
| **BB Lower** | ${bb_low:,.2f} |
| **MACD Hist** | {macd_hist:+.3f} |
| **Ticks** | {len(df)} |
| **Time** | {datetime.now().strftime('%H:%M:%S')} |
""")
            st.divider()

            st.markdown("**🔔 Alert Log**")
            if not st.session_state.alert_log:
                st.caption("لا توجد تنبيهات...")
            else:
                for a in list(st.session_state.alert_log)[:15]:
                    st.markdown(
                        f'<div class="{a["css"]}">'
                        f'<span style="color:#777;font-size:0.72rem">{a["time"]}</span><br>'
                        f'{a["icon"]} {a["msg"]}'
                        f'</div>', unsafe_allow_html=True)

    live_dashboard()


if __name__ == "__main__":
    main()