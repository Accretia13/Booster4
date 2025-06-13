# booster_all_in_one.py

import os
import re
import sys
import time
import math
import asyncio
import aiohttp
import aiosqlite
import sqlite3
import numpy as np
import pandas as pd
import requests
from tqdm import tqdm
from datetime import datetime, timedelta
import pytz

# === Папки ===
BASE = r"C:\Users\777\PycharmProjects\Booster4\scoring_p\datasets"
FOLDERS = {
    "3m": os.path.join(BASE, "3mtf"),
    "1h": os.path.join(BASE, "1htf"),
    "1d": os.path.join(BASE, "1dtf"),
}

# === Индикаторы ===
def wma(series, period):
    weights = list(range(1, period + 1))
    return series.rolling(period).apply(lambda x: sum(w * val for w, val in zip(weights, x)) / sum(weights), raw=True)

def hma(series, period):
    half = int(period / 2)
    sqrt_n = int(period ** 0.5)
    return wma(2 * wma(series, half) - wma(series, period), sqrt_n)

# === Вспомогательные ===
def from_ts_to_dt(ts: int) -> datetime:
    dt_utc = datetime.fromtimestamp(ts / 1000, tz=pytz.UTC)
    return dt_utc.astimezone(pytz.timezone("Europe/Moscow"))

def resample(df, rule, offset=None):
    df["datetime"] = pd.to_datetime(df["date"] + df["time"], format="%Y%m%d%H%M%S")
    df.set_index("datetime", inplace=True)
    if offset:
        df.index = df.index - offset
    agg = {"open": "first", "high": "max", "low": "min", "close": "last", "vol": "sum"}
    df_res = df.resample(rule).agg(agg).dropna()
    if offset:
        df_res.index = df_res.index + offset
    df_res["date"] = df_res.index.strftime("%Y%m%d")
    df_res["time"] = df_res.index.strftime("%H%M%S")
    return df_res.reset_index(drop=True)

def clean_folder(path):
    os.makedirs(path, exist_ok=True)
    for file in os.listdir(path):
        if file.endswith(".sqlite"):
            os.remove(os.path.join(path, file))

def save_to_sqlite(df, tf, ticker):
    folder = FOLDERS[tf]
    db_path = os.path.join(folder, f"{ticker}_{tf}.sqlite")
    if tf == "1d":
        columns = ["ticker", "per", "date", "time", "open", "high", "low", "close", "vol", "amplitude"]
    else:
        columns = ["ticker", "per", "date", "time", "open", "high", "low", "close", "vol",
                   "amplitude", "hma9", "hma21", "hma_cross"]
    df = df[columns]
    with sqlite3.connect(db_path) as conn:
        df.to_sql("candles", conn, if_exists="replace", index=False)


tickers_top = [
    "BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP", "DOGE-USDT-SWAP", "ANIME-USDT-SWAP",
    "PEPE-USDT-SWAP", "XRP-USDT-SWAP", "MASK-USDT-SWAP", "TRUMP-USDT-SWAP", "ADA-USDT-SWAP",
    "WLD-USDT-SWAP", "AAVE-USDT-SWAP", "KAITO-USDT-SWAP", "HYPE-USDT-SWAP", "FARTCOIN-USDT-SWAP",
    "AVAX-USDT-SWAP", "LTC-USDT-SWAP", "UNI-USDT-SWAP", "BCH-USDT-SWAP", "PNUT-USDT-SWAP",
    "LINK-USDT-SWAP", "BNB-USDT-SWAP", "CETUS-USDT-SWAP", "INJ-USDT-SWAP", "PEOPLE-USDT-SWAP",
    "LDO-USDT-SWAP", "AI16Z-USDT-SWAP", "FLM-USDT-SWAP", "ARB-USDT-SWAP", "AIXBT-USDT-SWAP",
    "TON-USDT-SWAP", "ICP-USDT-SWAP", "ZEREBRO-USDT-SWAP", "VINE-USDT-SWAP", "PI-USDT-SWAP",
    "SUI-USDT-SWAP", "DOT-USDT-SWAP", "MAJOR-USDT-SWAP", "JUP-USDT-SWAP", "DYDX-USDT-SWAP",
    "OP-USDT-SWAP", "NEAR-USDT-SWAP", "GALA-USDT-SWAP", "APT-USDT-SWAP", "ATOM-USDT-SWAP",
    "ORDI-USDT-SWAP", "XLM-USDT-SWAP", "FIL-USDT-SWAP", "STX-USDT-SWAP", "TIA-USDT-SWAP",
    "LPT-USDT-SWAP", "SAND-USDT-SWAP", "MOODENG-USDT-SWAP", "PYTH-USDT-SWAP", "NOT-USDT-SWAP"
]

# === Шаг 1: Загрузка и обработка котировок ===
async def fetch_and_save(session, sem, inst_id, index, total, tf="3m", limit=100, total_candles=3360):
    async with sem:
        ticker = inst_id.replace("-", "")
        after = ""
        candles = {}

        while len(candles) < total_candles:
            params = {"instId": inst_id, "bar": tf, "limit": str(limit)}
            if after: params["after"] = after
            url = "https://www.okx.com/api/v5/market/history-candles"

            for attempt in range(2):
                try:
                    async with session.get(url, params=params) as resp:
                        if resp.status != 200:
                            await asyncio.sleep(5)
                            continue
                        data = (await resp.json()).get("data", [])
                        if not data: break
                        for c in data:
                            ts = int(c[0])
                            if ts not in candles:
                                candles[ts] = {
                                    "ticker": ticker, "per": "3", "t": from_ts_to_dt(ts),
                                    "open": float(c[1]), "high": float(c[2]),
                                    "low": float(c[3]), "close": float(c[4]), "vol": float(c[7])
                                }
                        after = str(int(data[-1][0]))
                        await asyncio.sleep(0.25)
                        break
                except Exception:
                    await asyncio.sleep(10)

        df = pd.DataFrame(sorted(candles.values(), key=lambda x: x["t"]))
        df["date"] = df["t"].dt.strftime("%Y%m%d")
        df["time"] = df["t"].dt.strftime("%H%M%S")
        df.drop(columns=["t"], inplace=True)

        for timeframe, rule, per, offset in [("3m", None, "3", None), ("1h", "1h", "60", None), ("1d", "1d", "1440", pd.Timedelta(hours=3))]:
            dfx = df.copy() if timeframe == "3m" else resample(df.copy(), rule, offset)
            dfx["ticker"] = ticker
            dfx["per"] = per
            dfx["hma9"] = hma(dfx["close"], 9)
            dfx["hma21"] = hma(dfx["close"], 21)
            dfx["amplitude"] = 2 * (dfx["high"] - dfx["low"]) / (dfx["high"] + dfx["low"]) * 100
            dfx["hma_cross"] = 0
            prev9 = dfx["hma9"].shift(1)
            prev21 = dfx["hma21"].shift(1)
            dfx.loc[(prev9 < prev21) & (dfx["hma9"] > dfx["hma21"]), "hma_cross"] = 1
            dfx.loc[(prev9 > prev21) & (dfx["hma9"] < dfx["hma21"]), "hma_cross"] = -1
            save_to_sqlite(dfx, timeframe, ticker)

        bar_len = 30
        filled = int(bar_len * (index + 1) // total)
        bar = '█' * filled + '-' * (bar_len - filled)
        sys.stdout.write(f"\rProgress: |{bar}| {((index + 1) / total) * 100:.1f}%")
        sys.stdout.flush()

async def step1_download():
    for folder in FOLDERS.values():
        clean_folder(folder)
    print(f"🔎 Загружаем {len(tickers_top)} тикеров")
    sem = asyncio.Semaphore(5)
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        tasks = [fetch_and_save(session, sem, inst_id, i, len(tickers_top)) for i, inst_id in enumerate(tickers_top)]
        await asyncio.gather(*tasks)
    print("\n✅ Загрузка завершена")

# === Шаг 2: Z-оценка по тепловой карте ===
HEATMAP_PATH = r"C:\Users\777\PycharmProjects\Booster4\WarmMaps\RESULT_HEAT_MAP.xlsx"
DB_FOLDER = FOLDERS["1h"]
WEEKDAY_MAP = {0: "Пн", 1: "Вт", 2: "Ср", 3: "Чт", 4: "Пт", 5: "Сб", 6: "Вс"}

def load_heatmap(ticker: str) -> dict:
    try:
        df = pd.read_excel(HEATMAP_PATH, sheet_name=f"{ticker}_H1")
        df = df.loc[:, ~df.columns.str.contains("Среднее|Медиана")]
        return {(row["weekday_name"], hour): row[hour] for _, row in df.iterrows() for hour in df.columns[1:]}
    except Exception:
        return {}

def get_weekday_hour_key(date_str: str, time_str: str):
    try:
        dt = datetime.strptime(date_str + time_str, "%Y%m%d%H%M%S")
        return WEEKDAY_MAP[dt.weekday()], dt.strftime("%H:00")
    except:
        return None, None

def add_stats(df, heatmap):
    df["amplitude"] = pd.to_numeric(df["amplitude"], errors="coerce")

    # Историческая эффективность
    amp_mean_hist, zscores = [], []
    for date, time, amp in zip(df["date"], df["time"], df["amplitude"]):
        key = get_weekday_hour_key(str(date), str(time))
        mean = heatmap.get(key, np.nan)
        delta = amp - mean if pd.notna(mean) and pd.notna(amp) else np.nan
        amp_mean_hist.append(mean)
        zscores.append(delta)

    df["amp_mean_hist"] = amp_mean_hist
    df["zscore_delta"] = zscores

    # Текущая эффективность по последним 3 и 6 свечам
    df["amp_eff_last3"] = df["amplitude"].rolling(window=3).mean()
    df["amp_eff_last6"] = df["amplitude"].rolling(window=6).mean()

    return df

async def step2_enrich():
    files = [f for f in os.listdir(DB_FOLDER) if f.endswith(".sqlite")]
    sem = asyncio.Semaphore(6)

    async def process_file(file):
        async with sem:
            db_path = os.path.join(DB_FOLDER, file)
            ticker = file.replace("_1h.sqlite", "")
            heatmap = load_heatmap(ticker)
            if not heatmap: return
            async with aiosqlite.connect(db_path) as conn:
                cursor = await conn.execute("SELECT * FROM candles")
                columns = [col[0] for col in cursor.description]
                rows = await cursor.fetchall()
                df = pd.DataFrame(rows, columns=columns)
                df = add_stats(df, heatmap)
                await conn.execute("DROP TABLE IF EXISTS candles")
                cols = ",".join(f"{col} TEXT" for col in df.columns)
                await conn.execute(f"CREATE TABLE candles ({cols})")
                await conn.executemany(
                    f"INSERT INTO candles VALUES ({','.join(['?'] * len(df.columns))})",
                    df.values.tolist()
                )
                await conn.commit()

    await asyncio.gather(*[process_file(f) for f in files])
    print("\n✅ Обогащение завершено")

# === Шаг 3: Расчёт плотности HMA-cross ===
def compute_hma_cross(df):
    hma9 = pd.to_numeric(df["hma9"], errors="coerce")
    hma21 = pd.to_numeric(df["hma21"], errors="coerce")
    return ((hma9 > hma21) & (hma9.shift(1) <= hma21.shift(1))) | ((hma9 < hma21) & (hma9.shift(1) >= hma21.shift(1)))

def step3_density():
    from okx_downloader import process_3mtf, process_1htf, process_1dtf
    process_3mtf()
    process_1htf()
    process_1dtf()
    print("\n✅ Плотность HMA-кроссов рассчитана")

# === Полный пайплайн ===
async def full_pipeline():
    start_time = time.time()
    print("\n🔽 Шаг 1: Загрузка котировок с OKX...")
    await step1_download()
    print("\n📊 Шаг 2: Обогащение баз по тепловым картам...")
    await step2_enrich()
    print("\n📈 Шаг 3: Расчёт плотности HMA-кроссов...")
    step3_density()
    print(f"\n✅ Все этапы выполнены за {time.time() - start_time:.2f} секунд")

if __name__ == "__main__":
    asyncio.run(full_pipeline())
