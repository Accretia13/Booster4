import os
import pandas as pd
import numpy as np
from datetime import datetime
import pytz
import pandas as pd

def calculate_hma(series: pd.Series, period: int) -> pd.Series:
    """Вычисление Hull Moving Average"""
    half_length = int(period / 2)
    sqrt_length = int(period ** 0.5)

    wma_half = series.rolling(window=half_length).mean()
    wma_full = series.rolling(window=period).mean()

    hma = 2 * wma_half - wma_full
    hma = hma.rolling(window=sqrt_length).mean()
    return hma

# === Индикаторы ===
def calc_hma(series: pd.Series, period: int) -> pd.Series:
    wma = lambda s, p: s.rolling(p).apply(lambda x: np.average(x, weights=np.arange(1, p+1)), raw=True)
    half = int(period / 2)
    sqrt_len = int(np.sqrt(period))
    return wma(2 * wma(series, half) - wma(series, period), sqrt_len)

def detect_hma_cross(df: pd.DataFrame) -> str:
    hma9 = calc_hma(df['close'].astype(float), 9)
    hma21 = calc_hma(df['close'].astype(float), 21)
    if hma9.iloc[-2] < hma21.iloc[-2] and hma9.iloc[-1] > hma21.iloc[-1]:
        return 'long'
    elif hma9.iloc[-2] > hma21.iloc[-2] and hma9.iloc[-1] < hma21.iloc[-1]:
        return 'short'
    return ''

def compute_atr(df: pd.DataFrame, period: int) -> pd.Series:
    high = df['high'].astype(float)
    low = df['low'].astype(float)
    close = df['close'].astype(float)
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs()
    ], axis=1).max(axis=1)
    return tr.rolling(period).mean()

def get_volume_spike_flag(df: pd.DataFrame) -> bool:
    vol = df['volume'].astype(float)
    return vol.iloc[-1] > vol.rolling(20).mean().iloc[-1] * 1.5

def check_cross_confluence(df_3m: pd.DataFrame, df_1h: pd.DataFrame) -> bool:
    return detect_hma_cross(df_3m) == detect_hma_cross(df_1h)

# === Загрузка данных по тикеру ===

def load_ticker_data(data_dir, symbol):
    try:
        filepath = os.path.join(data_dir, f"{symbol}_3m.txt")
        cols = ["ticker", "per", "date", "time", "open", "high", "low", "close", "vol"]
        df = pd.read_csv(filepath, skiprows=1, names=cols)
        df = df.dropna(subset=["close"])
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df["open"] = pd.to_numeric(df["open"], errors="coerce")
        df["high"] = pd.to_numeric(df["high"], errors="coerce")
        df["low"] = pd.to_numeric(df["low"], errors="coerce")
        df["vol"] = pd.to_numeric(df["vol"], errors="coerce")
        return df
    except Exception as e:
        print(f"❌ Ошибка при загрузке {symbol}: {e}")
        return pd.DataFrame()

# === Контекст: heatmap и текущее время ===
def get_context(heatmap_df: pd.DataFrame) -> dict:
    now = datetime.now(pytz.timezone("Europe/Moscow"))
    hour_str = now.strftime("%H:00")
    weekday_str = now.strftime("%a")
    weekday_map = {
        "Mon": "Пн", "Tue": "Вт", "Wed": "Ср",
        "Thu": "Чт", "Fri": "Пт", "Sat": "Сб", "Sun": "Вс"
    }
    weekday_name = weekday_map[weekday_str]
    return {
        'heatmap': heatmap_df,
        'current_hour': hour_str,
        'current_weekday': weekday_name
    }

# === Скоринг монеты ===
def score_ticker(ticker_data, context):
    score = 0
    triggered_metrics = []

    df_3m = ticker_data["3m"]
    df_1h = ticker_data["1h"]
    df_1d = ticker_data["1d"]

    df_3m["hma_9"] = calculate_hma(df_3m["close"], 9)
    df_3m["hma_21"] = calculate_hma(df_3m["close"], 21)
    cross = (
        df_3m["hma_9"].iloc[-2] < df_3m["hma_21"].iloc[-2]
        and df_3m["hma_9"].iloc[-1] > df_3m["hma_21"].iloc[-1]
    )

    if cross:
        score += 1
        triggered_metrics.append("hma_cross")

    # ATR фильтр
    atr_21 = talib.ATR(df_3m["high"], df_3m["low"], df_3m["close"], timeperiod=21)
    min_amp = get_min_amp(context)
    amplitude = df_3m["high"].max() - df_3m["low"].min()
    if amplitude >= min_amp:
        score += 1
        triggered_metrics.append("amp_ok")

    # Volume фильтр
    vol_3m = df_3m["vol"].iloc[-1]
    vol_1h = df_1h["vol"].iloc[-1]
    vol_1d = df_1d["vol"].iloc[-1]
    if vol_3m > 50_000 and vol_1h > 500_000 and vol_1d > 2_000_000:
        score += 1
        triggered_metrics.append("vol_ok")

    # 📊 Отладка:
    symbol = df_3m["symbol"].iloc[-1] if "symbol" in df_3m.columns else "???"
    print(f"\n📊 {symbol}:")
    print(f"  HMA(9)[-1]={df_3m['hma_9'].iloc[-1]:.4f}, HMA(21)[-1]={df_3m['hma_21'].iloc[-1]:.4f}")
    print(f"  Пересечение: {cross}")
    print(f"  ATR: {atr_21.iloc[-1]:.4f}")
    print(f"  Амплитуда: {amplitude:.2f} vs min_amp: {min_amp:.2f}")
    print(f"  Объём 3m: {vol_3m:.2f}, 1h: {vol_1h:.2f}, 1d: {vol_1d:.2f}")

    return {"score": score, "triggered": triggered_metrics}


# === Основной запуск ===
# ... (весь импорт без изменений)

# === Основной запуск с отладкой ===
if __name__ == "__main__":
    heatmap_path = "C:/Users/777/PycharmProjects/pythonProject/WarmMaps/SUMMARY_TICKERS.xlsx"
    data_dir = "C:/Users/777/PycharmProjects/Booster4/data_live"

    heatmap = pd.read_excel(heatmap_path, index_col=1).iloc[:, 2:-2]
    print(f"🔍 Heatmap shape: {heatmap.shape}, columns: {list(heatmap.columns)}")

    file_names = os.listdir(data_dir)
    symbols = sorted(set(f.split("_")[0] for f in file_names if f.endswith("_3m.txt")))

    print(f"\n🔍 Found {len(symbols)} tickers: {symbols[:5]}...")

    print("\n[TOP SIGNALS]")
    for symbol in symbols:
        ticker_data = load_ticker_data(data_dir, symbol)

        # 🔍 Проверим наличие данных
        for tf, df in ticker_data.items():
            print(f"🔍 {symbol} {tf}: {len(df)} rows")

        context = get_context(heatmap)
        print(f"🔍 Time context: weekday={context['current_weekday']} hour={context['current_hour']}")

        try:
            result = score_ticker(ticker_data, context)
            print(f"{symbol}: score = {result['score']}, metrics = {result['triggered']}")
        except Exception as e:
            print(f"❌ Ошибка в тикере {symbol}: {e}")
