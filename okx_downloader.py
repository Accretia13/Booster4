import os
import sqlite3
import pandas as pd
import numpy as np
from tqdm import tqdm

# === Параметры ===
TF_PARAMS = {
    "3mtf": {
        "folder": r"C:\\Users\\777\\PycharmProjects\\Booster4\\scoring_p\\datasets\\3mtf",
        "window": 20,
        "time_column": "TIME",
        "date_column": "DATE",
        "start_minute": "00"
    },
    "1htf": {
        "folder": r"C:\\Users\\777\\PycharmProjects\\Booster4\\scoring_p\\datasets\\1htf",
        "window": 24,
        "time_column": "TIME",
        "date_column": "DATE",
        "start_hour": "03"
    },
    "1dtf": {
        "folder": r"C:\\Users\\777\\PycharmProjects\\Booster4\\scoring_p\\datasets\\1dtf",
        "source_3mtf": r"C:\\Users\\777\\PycharmProjects\\Booster4\\scoring_p\\datasets\\3mtf"
    }
}


def compute_hma_cross(df):
    df.columns = [col.lower().strip() for col in df.columns]
    hma9 = pd.to_numeric(df["hma9"], errors="coerce")
    hma21 = pd.to_numeric(df["hma21"], errors="coerce")
    cross = ((hma9 > hma21) & (hma9.shift(1) <= hma21.shift(1))) | ((hma9 < hma21) & (hma9.shift(1) >= hma21.shift(1)))
    return cross.astype(int)


def process_3mtf():
    p = TF_PARAMS["3mtf"]
    for file in tqdm(os.listdir(p["folder"]), desc="3mtf"):
        if not file.endswith(".sqlite"):
            continue
        path = os.path.join(p["folder"], file)
        con = sqlite3.connect(path)
        df = pd.read_sql_query("SELECT * FROM candles", con)
        df.columns = [col.lower().strip() for col in df.columns]

        cross_flags = compute_hma_cross(df)
        density = []
        for i in range(len(df)):
            if str(df.loc[i, p["time_column"].lower()])[-2:] == p["start_minute"]:
                window_crosses = cross_flags[max(0, i - p["window"]):i].sum()
                density.append(window_crosses)
            else:
                density.append(np.nan)

        df["density_hma_cross"] = density
        df.to_sql("candles", con, if_exists="replace", index=False)
        con.close()


def process_1htf():
    p = TF_PARAMS["1htf"]
    folder_3m = TF_PARAMS["3mtf"]["folder"]

    for file in tqdm(os.listdir(p["folder"]), desc="1htf"):
        if not file.endswith(".sqlite"):
            continue

        base_name = file.replace("_1h.sqlite", "")
        path_1h = os.path.join(p["folder"], file)
        path_3m = os.path.join(folder_3m, base_name + "_3m.sqlite")

        if not os.path.exists(path_3m):
            continue

        con_1h = sqlite3.connect(path_1h)
        con_3m = sqlite3.connect(path_3m)

        df_1h = pd.read_sql_query("SELECT * FROM candles", con_1h)
        df_3m = pd.read_sql_query("SELECT * FROM candles", con_3m)

        df_1h.columns = [col.lower().strip() for col in df_1h.columns]
        df_3m.columns = [col.lower().strip() for col in df_3m.columns]

        df_3m["hma_cross"] = compute_hma_cross(df_3m)

        # Часовой ключ: YYYYMMDDHH
        df_3m["datetime"] = pd.to_datetime(df_3m["date"] + df_3m["time"], format="%Y%m%d%H%M%S")
        df_3m["hour_key"] = df_3m["datetime"].dt.strftime("%Y%m%d%H")
        df_3m["minute"] = df_3m["datetime"].dt.minute

        # Фильтруем на начало каждого часа (MM == 00)
        df_hourly = df_3m[df_3m["minute"] == 0]
        density = df_3m[df_3m["hma_cross"] != 0].groupby("hour_key").size()

        df_1h["datetime"] = pd.to_datetime(df_1h["date"] + df_1h["time"], format="%Y%m%d%H%M%S")
        df_1h["hour_key"] = df_1h["datetime"].dt.strftime("%Y%m%d%H")
        df_1h["density_hma_cross"] = df_1h["hour_key"].map(density).fillna(0).astype(int)

        df_1h.drop(columns=["datetime", "hour_key"], inplace=True)
        df_1h.to_sql("candles", con_1h, if_exists="replace", index=False)

        con_1h.close()
        con_3m.close()



def process_1dtf():
    p = TF_PARAMS["1dtf"]
    folder = p["folder"]
    folder_1h = TF_PARAMS["1htf"]["folder"]

    for file in tqdm(os.listdir(folder), desc="1dtf"):
        if not file.endswith(".sqlite"):
            continue

        path_1d = os.path.join(folder, file)
        base_name = file.replace("_1d.sqlite", "")
        path_1h = os.path.join(folder_1h, base_name + "_1h.sqlite")

        if not os.path.exists(path_1h):
            continue

        # === Загрузка дневных и часовых свечей ===
        con_day = sqlite3.connect(path_1d)
        con_hour = sqlite3.connect(path_1h)

        df_day = pd.read_sql_query("SELECT * FROM candles", con_day)
        df_hour = pd.read_sql_query("SELECT * FROM candles", con_hour)

        con_day.close()
        con_hour.close()

        # === Очистка HMA-столбцов ===
        for col in ["hma9", "hma21", "hma_cross"]:
            if col in df_day.columns:
                df_day.drop(columns=[col], inplace=True)

        # === Расчёт amp_eff_avg по дневной дате ===
        df_hour.columns = [c.lower().strip() for c in df_hour.columns]
        df_hour["date"] = df_hour["date"].astype(str)
        df_hour["amp_eff"] = pd.to_numeric(df_hour.get("amplitude"), errors="coerce")

        amp_eff_by_date = df_hour.groupby("date")["amp_eff"].mean().to_dict()
        df_day["amp_eff_avg"] = df_day["date"].astype(str).map(amp_eff_by_date)

        # === Сохранение обратно ===
        con_day = sqlite3.connect(path_1d)
        df_day.to_sql("candles", con_day, if_exists="replace", index=False)
        con_day.close()


if __name__ == "__main__":
    process_3mtf()
    process_1htf()
    process_1dtf()