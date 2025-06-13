import os
import re
import asyncio
import aiohttp
import sqlite3
from datetime import datetime, timedelta
import pytz
import requests
import time
import sys
import pandas as pd

# === ПАПКИ ===
BASE = r"C:\\Users\\777\\PycharmProjects\\Booster4\\scoring_p\\datasets"
FOLDERS = {
    "3m": os.path.join(BASE, "3mtf"),
    "1h": os.path.join(BASE, "1htf"),
    "1d": os.path.join(BASE, "1dtf"),
}

# === Очистка папок ===
def clean_folder(path):
    os.makedirs(path, exist_ok=True)
    for file in os.listdir(path):
        if file.endswith(".sqlite"):
            os.remove(os.path.join(path, file))

# === Индикаторы ===
def wma(series, period):
    weights = list(range(1, period + 1))
    return series.rolling(period).apply(lambda x: sum(w * val for w, val in zip(weights, x)) / sum(weights), raw=True)

def hma(series, period):
    half = int(period / 2)
    sqrt_n = int(period ** 0.5)
    wma_half = wma(series, half)
    wma_full = wma(series, period)
    hma_val = 2 * wma_half - wma_full
    return wma(hma_val, sqrt_n)

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

# # === Получение тикеров ===
# def get_active_usdt_swap_tickers(threshold_usdt=10_000_000):
#     instruments_url = "https://www.okx.com/api/v5/public/instruments"
#     instruments = requests.get(instruments_url, params={"instType": "SWAP"}).json()["data"]
#
#     tickers_url = "https://www.okx.com/api/v5/market/tickers"
#     tickers_data = requests.get(tickers_url, params={"instType": "SWAP"}).json()["data"]
#     volumes = {item["instId"]: float(item["volCcy24h"]) for item in tickers_data}
#
#     return [
#         item["instId"]
#         for item in instruments
#         if item["instId"].endswith("USDT-SWAP") and volumes.get(item["instId"], 0.0) >= threshold_usdt
#     ]

# === Вспомогательные ===
def from_ts_to_dt(ts: int) -> datetime:
    dt_utc = datetime.fromtimestamp(ts / 1000, tz=pytz.UTC)
    return dt_utc.astimezone(pytz.timezone("Europe/Moscow"))

def resample(df, rule, offset=None):
    df["datetime"] = pd.to_datetime(df["date"] + df["time"], format="%Y%m%d%H%M%S")
    df.set_index("datetime", inplace=True)
    if offset:
        df.index = df.index - offset
    agg = {
        "open": "first", "high": "max", "low": "min", "close": "last", "vol": "sum"
    }
    df_res = df.resample(rule).agg(agg).dropna()
    if offset:
        df_res.index = df_res.index + offset
    df_res["date"] = df_res.index.strftime("%Y%m%d")
    df_res["time"] = df_res.index.strftime("%H%M%S")
    return df_res.reset_index(drop=True)

# === Сохранение в SQLite ===
def save_to_sqlite(df, tf, ticker):
    folder = FOLDERS[tf]
    db_path = os.path.join(folder, f"{ticker}_{tf}.sqlite")
    columns = ["ticker", "per", "date", "time", "open", "high", "low", "close", "vol",
               "amplitude", "hma9", "hma21", "hma_cross"]
    df = df[columns]  # Упорядочим строго
    with sqlite3.connect(db_path) as conn:
        df.to_sql("candles", conn, if_exists="replace", index=False)

# === Загрузка и обработка ===
async def fetch_and_save(session, sem, inst_id, index, total, tf="3m", limit=100, total_candles=3360):
    async with sem:
        ticker = inst_id.replace("-", "")
        after = ""
        candles = {}

        while len(candles) < total_candles:
            params = {
                "instId": inst_id,
                "bar": tf,
                "limit": str(limit),
            }
            if after:
                params["after"] = after
            url = "https://www.okx.com/api/v5/market/history-candles"

            max_retries = 2
            attempt = 0
            success = False
            data = []
            while attempt < max_retries:
                try:
                    async with session.get(url, params=params) as resp:
                        if resp.status != 200:
                            print(f"\n❌ Ошибка {resp.status} для {inst_id}, попытка {attempt + 1}/{max_retries}")
                            attempt += 1
                            await asyncio.sleep(5)
                            continue
                        data = (await resp.json()).get("data", [])
                        success = True
                        break
                except Exception as e:
                    print(f"\n⚠️ Ошибка запроса {inst_id}, попытка {attempt + 1}/{max_retries}: {e}")
                    attempt += 1
                    await asyncio.sleep(10)

            if not success or not data:
                print(f"\n⚠️ Не удалось получить данные для {inst_id} после {max_retries} попыток")
                return

            for c in data:
                ts = int(c[0])
                if ts not in candles:
                    candles[ts] = {
                        "ticker": ticker,
                        "per": "3",
                        "t": from_ts_to_dt(ts),
                        "open": float(c[1]),
                        "high": float(c[2]),
                        "low": float(c[3]),
                        "close": float(c[4]),
                        "vol": float(c[7])
                    }
            after = str(int(data[-1][0]))
            await asyncio.sleep(0.25)

        if not candles:
            print(f"\n⚠️ Нет данных для {inst_id}")
            return

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
        percent = (index + 1) / total * 100
        sys.stdout.write(f"\rProgress: |{bar}| {percent:.1f}% ({index + 1}/{total})")
        sys.stdout.flush()

# === Главная ===
async def main():
    for folder in FOLDERS.values():
        clean_folder(folder)

    inst_ids = tickers_top
    print(f"🔎 Найдено {len(inst_ids)} активных тикеров")

    sem = asyncio.Semaphore(5)
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        tasks = [fetch_and_save(session, sem, inst_id, i, len(inst_ids)) for i, inst_id in enumerate(inst_ids)]
        await asyncio.gather(*tasks)
    print("\n✅ Завершено")

if __name__ == "__main__":
    start_time = time.time()
    asyncio.run(main())
    end_time = time.time()
    elapsed = end_time - start_time
    print(f"\n🕒 Общее время работы скрипта: {elapsed:.2f} секунд")
