import os
import sqlite3
import pandas as pd
from datetime import datetime
import numpy as np
import asyncio
import aiosqlite
import sys

# === Константы ===
WEEKDAY_MAP = {
    0: "Пн", 1: "Вт", 2: "Ср", 3: "Чт", 4: "Пт", 5: "Сб", 6: "Вс"
}

HEATMAP_PATH = r"C:\Users\777\PycharmProjects\Booster4\WarmMaps\RESULT_HEAT_MAP.xlsx"
DB_FOLDER = r"C:\Users\777\PycharmProjects\Booster4\scoring_p\datasets\1htf"
STD_ESTIMATE = 0.15
CONCURRENCY = 6

# === Загрузка тепловой карты ===
def load_heatmap(ticker: str) -> dict:
    sheet_name = f"{ticker}_H1"
    try:
        df = pd.read_excel(HEATMAP_PATH, sheet_name=sheet_name)
    except Exception as e:
        print(f"❌ {ticker}: не удалось загрузить тепловую карту {sheet_name}: {e}")
        return {}

    df = df.loc[:, ~df.columns.str.contains("Среднее|Медиана")]
    heatmap = {}
    for _, row in df.iterrows():
        weekday = row["weekday_name"]
        for hour in df.columns[1:]:
            heatmap[(weekday, hour)] = row[hour]
    return heatmap

# === Получить (день недели, час) по дате и времени ===
def get_weekday_hour_key(date_str: str, time_str: str) -> tuple:
    try:
        dt = datetime.strptime(date_str + time_str, "%Y%m%d%H%M%S")
        return WEEKDAY_MAP[dt.weekday()], dt.strftime("%H:00")
    except:
        return None, None

# === Добавить исторические значения и дельту ===
def add_stats(df: pd.DataFrame, heatmap: dict) -> pd.DataFrame:
    df["amplitude"] = pd.to_numeric(df["amplitude"], errors="coerce")
    means, deltas = [], []
    for date, time, amp in zip(df["date"], df["time"], df["amplitude"]):
        key = get_weekday_hour_key(str(date), str(time))
        if key is None:
            mean, delta = np.nan, np.nan
        else:
            mean = heatmap.get(key, np.nan)
            delta = amp - mean if pd.notna(mean) and pd.notna(amp) else np.nan
        means.append(mean)
        deltas.append(delta)
    df["amp_mean_hist"] = means
    df["zscore_delta"] = deltas
    return df

# === Асинхронная обработка одного файла ===
async def process_file(file: str, sem: asyncio.Semaphore, index: int, total: int):
    async with sem:
        db_path = os.path.join(DB_FOLDER, file)
        ticker = file.replace("_1h.sqlite", "")
        try:
            heatmap = load_heatmap(ticker)
            if not heatmap:
                return

            async with aiosqlite.connect(db_path) as conn:
                cursor = await conn.execute("SELECT * FROM candles")
                columns = [col[0] for col in cursor.description]
                rows = await cursor.fetchall()
                df = pd.DataFrame(rows, columns=columns)

            df = add_stats(df, heatmap)

            async with aiosqlite.connect(db_path) as conn:
                await conn.execute("DROP TABLE IF EXISTS candles")
                cols = ",".join(f"{col} TEXT" for col in df.columns)
                await conn.execute(f"CREATE TABLE candles ({cols})")
                await conn.executemany(
                    f"INSERT INTO candles VALUES ({','.join(['?']*len(df.columns))})",
                    df.values.tolist()
                )
                await conn.commit()

            percent = int((index + 1) / total * 100)
            bar = '█' * (percent // 2) + '-' * (50 - percent // 2)
            sys.stdout.write(f"\r[{bar}] {percent}% ({index + 1}/{total}) — {ticker}")
            sys.stdout.flush()

        except Exception as e:
            print(f"\n❌ Ошибка в {ticker}: {e}")

# === Основной запуск ===
async def main():
    files = [f for f in os.listdir(DB_FOLDER) if f.endswith(".sqlite")]
    print(f"🔍 Найдено {len(files)} файлов. Начинаем обработку...\n")

    sem = asyncio.Semaphore(CONCURRENCY)
    tasks = [process_file(file, sem, i, len(files)) for i, file in enumerate(files)]
    await asyncio.gather(*tasks)

    print("\n🏁 Обработка завершена.")

if __name__ == "__main__":
    asyncio.run(main())
