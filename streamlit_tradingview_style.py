import streamlit as st
import pandas as pd
import sqlite3
import os
import plotly.graph_objects as go
import plotly.express as px

# Настройки страницы
st.set_page_config(page_title="TradingView-style Dashboard", layout="wide")
st.header("🕯️ TradingView-style дашборд с HMA и сигналами")

# Пути к данным и мэппинг таймфреймов
BASE_PATH = "C:/Users/777/PycharmProjects/Booster4/scoring_p/datasets"
TF_MAP = {"3m": "3mtf", "1h": "1htf", "1d": "1dtf"}

# Sidebar: выбор таймфрейма и тикера
tf = st.sidebar.selectbox("Выбери таймфрейм", list(TF_MAP.keys()), index=1)
data_dir = os.path.join(BASE_PATH, TF_MAP[tf])
tickers = sorted({f.split('_')[0] for f in os.listdir(data_dir) if f.endswith(".sqlite")})
ticker = st.sidebar.selectbox("Выбери тикер", tickers)

# Функции для загрузки данных
@st.cache_data
def load_data(ticker, tf):
    path = os.path.join(BASE_PATH, TF_MAP[tf], f"{ticker}_{tf}.sqlite")
    conn = sqlite3.connect(path)
    df = pd.read_sql("SELECT * FROM candles", conn)
    conn.close()
    df["datetime"] = pd.to_datetime(df["date"] + " " + df["time"], errors="coerce")
    for col in df.columns.difference(["ticker","per","date","time","datetime"]):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.sort_values("datetime")

@st.cache_data
def load_data_3m(ticker):
    path = os.path.join(BASE_PATH, TF_MAP["3m"], f"{ticker}_3m.sqlite")
    conn = sqlite3.connect(path)
    df3 = pd.read_sql("SELECT * FROM candles", conn)
    conn.close()
    df3["datetime"] = pd.to_datetime(df3["date"] + " " + df3["time"], errors="coerce")
    for col in df3.columns.difference(["ticker","per","date","time","datetime"]):
        df3[col] = pd.to_numeric(df3[col], errors="coerce")
    return df3.sort_values("datetime")

# Загрузка данных
df = load_data(ticker, tf)
df3m = load_data_3m(ticker)

# Фильтр по дате
st.sidebar.markdown("### ⏳ Фильтр по дате")
start = st.sidebar.date_input("С", df["datetime"].dt.date.min())
end   = st.sidebar.date_input("По", df["datetime"].dt.date.max())
df = df[(df["datetime"].dt.date >= start) & (df["datetime"].dt.date <= end)]
df3m = df3m[(df3m["datetime"].dt.date >= start) & (df3m["datetime"].dt.date <= end)]

# Фильтр сигналов HMA_cross для 1h
if "hma_cross" in df.columns:
    selected_signals = st.sidebar.multiselect("Фильтр сигналов HMA_cross", [-1, 0, 1], default=[-1, 1])
    df_signals = df[df["hma_cross"].isin(selected_signals)]
else:
    df_signals = pd.DataFrame()

# 1️⃣ TradingView-style дашборд (цена + HMA + сигналы)
st.subheader("📈 Цена и HMA")
fig = go.Figure()
# Подготовка hovertext с амплитудой и данными OHLC
hover_texts = df.apply(
    lambda r: f"Time: {r['datetime']}<br>O: {r['open']}<br>H: {r['high']}<br>L: {r['low']}<br>C: {r['close']}<br>Amp: {r.get('amplitude', '')}",
    axis=1
)
fig.add_trace(go.Candlestick(
    x=df["datetime"], open=df["open"], high=df["high"],
    low=df["low"], close=df["close"], name="OHLC",
    hovertext=hover_texts,
    hoverinfo="text"
))
# HMA линии
for hma in ["hma9", "hma21"]:
    if hma in df.columns:
        fig.add_trace(go.Scatter(
            x=df["datetime"], y=df[hma], mode="lines", name=hma.upper(), hoverinfo="none"
        ))
# Вертикальные линии сигналов на 1h
if tf == "1h" and not df_signals.empty:
    for _, row in df_signals[df_signals["hma_cross"] != 0].iterrows():
        color = "green" if row["hma_cross"] == 1 else "red"
        fig.add_shape(
            type="line", x0=row["datetime"], x1=row["datetime"],
            y0=0, y1=1, yref="paper", xref="x",
            line=dict(color=color, width=2)
        )
# Навигация и горизонт/вертикальный зум
fig.update_layout(
    xaxis=dict(rangeslider=dict(visible=True), type="date"),
    yaxis=dict(type="log"),        # <-- вот это делает ось Y логарифмической
    dragmode="zoom",
    height=1200
)

st.plotly_chart(
    fig,
    use_container_width=True,
    config={
        'scrollZoom': True,
        'displayModeBar': True
    }
)

# 2️⃣ Последние 6 строк
st.subheader("🖥️ Последние 6 строк")
st.dataframe(df.tail(6))

# 3️⃣ 🔥 Amplitude с маркерами и диапазоном
if "amplitude" in df.columns:
    st.subheader("🔥 Amplitude")
    fig_am = px.line(
        df, x="datetime", y="amplitude", markers=True, title="Amplitude over time"
    )
    fig_am.update_layout(
        xaxis=dict(rangeslider=dict(visible=True), type="date"),
        height=1000
    )
    st.plotly_chart(
        fig_am,
        use_container_width=True,
        config={'scrollZoom': True, 'displayModeBar': True}
    )

# 4️⃣ Amp_eff_last3 и Amp_eff_last6 на одном графике
if "amp_eff_last3" in df.columns and "amp_eff_last6" in df.columns:
    st.subheader("📊 Amp_eff_last3 (красный) и Amp_eff_last6 (синий)")
    fig_eff = go.Figure()
    fig_eff.add_trace(go.Scatter(
        x=df["datetime"], y=df["amp_eff_last3"], mode="lines+markers",
        name="amp_eff_last3", line=dict(color="red"), marker=dict(color="red")
    ))
    fig_eff.add_trace(go.Scatter(
        x=df["datetime"], y=df["amp_eff_last6"], mode="lines+markers",
        name="amp_eff_last6", line=dict(color="#40E0D0"), marker=dict(color="#40E0D0")
    ))
    fig_eff.update_layout(
        xaxis=dict(rangeslider=dict(visible=True), type="date"),
        height=1000
    )
    st.plotly_chart(
        fig_eff,
        use_container_width=True,
        config={'scrollZoom': True, 'displayModeBar': True}
    )

# 5️⃣ ⌚ Hourly HMA_cross count (3m)
st.subheader("⌚ Плотность HMA_cross по часам (3m)")
hourly = (
    df3m.assign(hour=df3m["datetime"].dt.floor("H"))
    .groupby("hour")["hma_cross"]
    .apply(lambda x: (x != 0).sum())
    .reset_index(name="count")
)
fig_hr = px.bar(
    hourly, x="hour", y="count", text="count",
    labels={"hour":"Hour","count":"Count"},
    title="Hourly HMA_cross count (3m)"
)
fig_hr.update_layout(
    xaxis=dict(rangeslider=dict(visible=True), type="date"),
    height=1000
)
st.plotly_chart(
    fig_hr,
    use_container_width=True,
    config={'scrollZoom': True, 'displayModeBar': True}
)

# 6️⃣ 📅 Daily HMA_cross count (3m)
st.subheader("📅 Ежедневное количество HMA_cross (3m)")
daily = (
    df3m.assign(date=df3m["datetime"].dt.date)
    .groupby("date")["hma_cross"]
    .apply(lambda x: (x != 0).sum())
    .reset_index(name="count")
)
fig_daily = px.bar(
    daily, x="date", y="count", text="count",
    labels={"date":"Date","count":"Count"},
    title="Daily HMA_cross count (3m)"
)
fig_daily.update_layout(
    xaxis=dict(rangeslider=dict(visible=True), type="date"),
    height=1000
)
st.plotly_chart(
    fig_daily,
    use_container_width=True,
    config={'scrollZoom': True, 'displayModeBar': True}
)
