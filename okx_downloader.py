import httpx
import os
import pandas as pd
from datetime import datetime, timedelta, timezone
import time
import asyncio
from asyncio import Semaphore

# === Параметры ===
BASE_URL = "https://www.okx.com"
CANDLE_ENDPOINT = "/api/v5/market/candles"
OUTPUT_DIR = "./data_live"
VOL_THRESHOLD_M = 30.0
MAX_CONCURRENT_REQUESTS = 3  # Снижен лимит
TFS = {
    "3m": {"bar": "3m", "limit": 100, "filename": "_3m.txt", "per": 3},
    "1h": {"bar": "1H", "limit": 100, "filename": "_1h.txt", "per": 60},
    "1d": {"bar": "1Dutc", "limit": 100, "filename": "_1d.txt", "per": 1440},
}

os.makedirs(OUTPUT_DIR, exist_ok=True)

# === Получение ликвидных тикеров ===
def get_liquid_tickers_vol_millions(vol_threshold_m=30.0):
    url = "https://www.okx.com/api/v5/market/tickers?instType=SWAP"
    try:
        response = httpx.get(url, timeout=10.0)
        response.raise_for_status()
        data = response.json().get("data", [])
        total_tickers = len(data)

        filtered = []
        for item in data:
            inst_id = item.get("instId", "")
            vol_ccy_24h = float(item.get("volCcy24h", 0.0))
            vol_millions = vol_ccy_24h / 1_000_000
            if inst_id.endswith("USDT-SWAP") and vol_millions > vol_threshold_m:
                transformed_id = inst_id.replace("-", "").upper()
                filtered.append({
                    "instId": inst_id,
                    "transformedId": transformed_id,
                    "volCcy24h_M": round(vol_millions, 3)
                })

        df = pd.DataFrame(filtered).sort_values(by="volCcy24h_M", ascending=False).reset_index(drop=True)
        print(f"[INFO] Всего тикеров получено: {total_tickers}")
        print(f"[INFO] Тикеров с объемом > {vol_threshold_m} млн $ в сутки: {len(filtered)}\n")
        return df

    except Exception as e:
        print(f"[ERROR] Ошибка при получении тикеров: {e}")
        return pd.DataFrame(columns=["instId", "transformedId", "volCcy24h_M"])

# === Асинхронная загрузка и сохранение свечей с авто-повтором ===
async def fetch_and_save_candles(inst_id: str, symbol_name: str, semaphore: Semaphore, client: httpx.AsyncClient):
    header = "<TICKER>,<PER>,<DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>"
    async with semaphore:
        for tf, tf_data in TFS.items():
            params = {
                "instId": inst_id,
                "bar": tf_data["bar"],
                "limit": tf_data["limit"]
            }
            retries = 3
            for attempt in range(retries):
                try:
                    resp = await client.get(BASE_URL + CANDLE_ENDPOINT, params=params, timeout=10.0)
                    if resp.status_code == 429:
                        raise httpx.HTTPStatusError("Too Many Requests", request=resp.request, response=resp)
                    resp.raise_for_status()
                    candles = resp.json()["data"]
                    candles.reverse()

                    lines = []
                    for entry in candles:
                        ts_utc = datetime.fromtimestamp(float(entry[0]) / 1000, tz=timezone.utc)
                        ts_msk = ts_utc + timedelta(hours=3)
                        # Привязка дня к 03:00 МСК (UTC+3)
                        if tf.startswith("1d"):
                            msk_day_boundary = ts_msk.replace(hour=3, minute=0, second=0, microsecond=0)
                            if ts_msk < msk_day_boundary:
                                ts_msk = msk_day_boundary - timedelta(days=1)
                            else:
                                ts_msk = msk_day_boundary

                        date_str = ts_msk.strftime("%Y%m%d")
                        time_str = ts_msk.strftime("%H%M%S")

                        open_p = float(entry[1])
                        vol_base = float(entry[5])
                        vol_quote = round(open_p * vol_base, 2)  # VOL в quote валюте (USDT)

                        line = f"{symbol_name},{tf_data['per']},{date_str},{time_str},{entry[1]},{entry[2]},{entry[3]},{entry[4]},{vol_quote}"
                        lines.append(line)

                    filename = f"{symbol_name}{tf_data['filename']}"
                    filepath = os.path.join(OUTPUT_DIR, filename)
                    with open(filepath, "w", encoding="utf-8") as f:
                        f.write(header + "\n" + "\n".join(lines))

                    break

                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 429:
                        print(f"[!] 429 от OKX: {symbol_name} | {tf} — повтор через 2с...")
                        await asyncio.sleep(2.0)
                    else:
                        print(f"[!] Ошибка {symbol_name} | {tf}: {e}")
                        break
                except Exception as e:
                    print(f"[!] Ошибка: {symbol_name} | {tf} → {e}")
                    break
        await asyncio.sleep(0.25)

# === Основной запуск ===
async def main():
    start_time = time.time()
    tickers_df = get_liquid_tickers_vol_millions(VOL_THRESHOLD_M)
    total = len(tickers_df)

    semaphore = Semaphore(MAX_CONCURRENT_REQUESTS)
    tasks = []

    async with httpx.AsyncClient() as client:
        for i, row in tickers_df.iterrows():
            inst_id = row["instId"]
            symbol_name = row["transformedId"]
            task = fetch_and_save_candles(inst_id, symbol_name, semaphore, client)
            tasks.append(task)

        for i in range(0, len(tasks), 3):
            await asyncio.gather(*tasks[i:i+3])
            percent = int((i + 3) / total * 100)
            print(f"[PROGRESS] {min(percent, 100)}% завершено")

    duration = round(time.time() - start_time, 2)
    print(f"\n[FINISHED] Время выполнения: {duration} секунд")

async def download_and_save_all_candles(vol_threshold: float = VOL_THRESHOLD_M) -> list:
    start_time = time.time()
    tickers_df = get_liquid_tickers_vol_millions(vol_threshold)
    total = len(tickers_df)

    if tickers_df.empty:
        print("[ERROR] Нет тикеров для загрузки")
        return []

    semaphore = Semaphore(MAX_CONCURRENT_REQUESTS)
    tasks = []

    async with httpx.AsyncClient() as client:
        for _, row in tickers_df.iterrows():
            inst_id = row["instId"]
            symbol_name = row["transformedId"]
            task = fetch_and_save_candles(inst_id, symbol_name, semaphore, client)
            tasks.append(task)

        for i in range(0, len(tasks), MAX_CONCURRENT_REQUESTS):
            await asyncio.gather(*tasks[i:i + MAX_CONCURRENT_REQUESTS])
            percent = int((i + MAX_CONCURRENT_REQUESTS) / total * 100)
            print(f"[PROGRESS] {min(percent, 100)}% завершено")

    duration = round(time.time() - start_time, 2)
    print(f"\n[FINISHED] Загружено {len(tickers_df)} тикеров за {duration} секунд")

    return tickers_df["transformedId"].tolist()

if __name__ == "__main__":
    asyncio.run(main())