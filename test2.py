import os
import re
import asyncio
import aiohttp
from datetime import datetime
import pytz
import requests
import time
import sys

# === Получение тикеров с volCcy24h >= 10 млн USDT ===
def get_active_usdt_swap_tickers(threshold_usdt=10_000_000):
    instruments_url = "https://www.okx.com/api/v5/public/instruments"
    instruments = requests.get(instruments_url, params={"instType": "SWAP"}).json()["data"]

    tickers_url = "https://www.okx.com/api/v5/market/tickers"
    tickers_data = requests.get(tickers_url, params={"instType": "SWAP"}).json()["data"]
    volumes = {item["instId"]: float(item["volCcy24h"]) for item in tickers_data}

    return [
        item["instId"]
        for item in instruments
        if item["instId"].endswith("USDT-SWAP") and volumes.get(item["instId"], 0.0) >= threshold_usdt
    ]

# === Вспомогательные функции ===
def from_ts_to_dt(ts: int) -> datetime:
    dt_utc = datetime.fromtimestamp(ts / 1000, tz=pytz.UTC)
    return dt_utc.astimezone(pytz.timezone("Europe/Moscow"))

def get_last_saved_ts(file_name):
    if not os.path.exists(file_name):
        return ""
    with open(file_name, "r", encoding="utf-8") as f:
        lines = f.readlines()
    for line in reversed(lines):
        if line and not line.startswith("<"):
            parts = line.strip().split(",")
            if len(parts) >= 4:
                date, time_ = parts[2], parts[3]
                dt = datetime.strptime(date + time_, "%Y%m%d%H%M%S")
                ts = int(dt.replace(tzinfo=pytz.timezone("Europe/Moscow")).timestamp() * 1000)
                return str(ts)
    return ""

# === Асинхронная загрузка свечей ===
async def fetch_and_save(session, sem, inst_id, index, total, tf="3m", limit=100, total_candles=3360):
    async with sem:
        ticker = inst_id.replace("-", "")
        per = re.findall(r"\d+", tf)[0]

        output_dir = os.path.join("scoring_p", "datasets")
        os.makedirs(output_dir, exist_ok=True)
        file_name = os.path.join(output_dir, f"{ticker}_{tf}_{total_candles}.txt")

        after = get_last_saved_ts(file_name)
        candles = {}
        page = 0

        while len(candles) < total_candles:
            params = {
                "instId": inst_id,
                "bar": tf,
                "limit": str(limit),
            }
            if after:
                params["after"] = after

            url = "https://www.okx.com/api/v5/market/history-candles"

            try:
                async with session.get(url, params=params) as resp:
                    if resp.status != 200:
                        print(f"\n❌ Ошибка {resp.status} для {inst_id}")
                        break
                    data = (await resp.json()).get("data", [])
            except Exception as e:
                print(f"\n⚠️ Ошибка запроса {inst_id}: {e}")
                break

            if not data:
                break

            for c in data:
                ts = int(c[0])
                if ts not in candles:
                    candles[ts] = {
                        "t": from_ts_to_dt(ts),
                        "o": float(c[1]),
                        "h": float(c[2]),
                        "l": float(c[3]),
                        "c": float(c[4]),
                        "v": float(c[7]),  # volCcyQuote
                    }

            after = str(int(data[-1][0]))  # paginate earlier
            if len(data) < limit:
                break

            page += 1
            await asyncio.sleep(0.25)

        # Сортировка и сохранение
        ts_all = sorted(candles.keys(), reverse=True)[:total_candles]
        ts_all.sort()

        with open(file_name, "w", encoding="utf-8") as f:
            f.write("<TICKER>,<PER>,<DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>\n")
            for ts in ts_all:
                c = candles[ts]
                date = c["t"].strftime("%Y%m%d")
                time_ = c["t"].strftime("%H%M%S")
                f.write(f"{ticker},{per},{date},{time_},{c['o']},{c['h']},{c['l']},{c['c']},{c['v']}\n")

        # Прогресс-бар
        bar_length = 30
        filled_length = int(bar_length * (index + 1) // total)
        bar = '█' * filled_length + '-' * (bar_length - filled_length)
        percent = (index + 1) / total * 100
        sys.stdout.write(f"\rProgress: |{bar}| {percent:.1f}% ({index + 1}/{total})")
        sys.stdout.flush()

# === Главная функция ===
async def main():
    start_time = time.time()
    inst_ids = get_active_usdt_swap_tickers()
    total = len(inst_ids)
    print(f"🔎 Найдено {total} активных тикеров")

    sem = asyncio.Semaphore(5)
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        tasks = [fetch_and_save(session, sem, inst_id, i, total, tf="3m", total_candles=3360) for i, inst_id in enumerate(inst_ids)]
        await asyncio.gather(*tasks)

    elapsed = time.time() - start_time
    print(f"\n✅ Завершено за {elapsed:.2f} секунд")

if __name__ == "__main__":
    asyncio.run(main())
