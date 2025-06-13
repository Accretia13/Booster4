import sqlite3
from pathlib import Path
import pandas as pd

# Путь к директории с SQLite-файлами
ONEH_DIR = Path(r"C:\Users\777\PycharmProjects\Booster4\scoring_p\datasets\1htf")

results = []
for db_file in ONEH_DIR.glob("*_1h.sqlite"):
    # сначала вытаскиваем имя, убираем _1h, потом убираем суффикс USDTSWAP
    raw = db_file.stem.replace("_1h", "")
    ticker = raw.removesuffix("USDTSWAP")

    with sqlite3.connect(db_file) as conn:
        df = pd.read_sql_query("SELECT amp_eff_last3 FROM candles", conn)

    s = pd.to_numeric(df['amp_eff_last3'], errors='coerce').dropna().iloc[3:]
    q1, med, q3, q90 = s.quantile([0.25, 0.50, 0.75, 0.90])

    results.append({
        'ticker': ticker,
        'Q1':       q1,
        'MEDIAN':   med,
        'Q3':       q3,
        'Q90':      q90
    })

df_thresholds = (
    pd.DataFrame(results)
      .set_index('ticker')
      .sort_index()
      .round(2)
)

first_coins = [
    'BTC','ETH','AVAX','ATOM',
    'ADA','DOGE','AAVE','TRUMP',
    'UNI','OP','ARB',
]
first_present = [c for c in first_coins if c in df_thresholds.index]
others = df_thresholds.drop(index=first_present, errors='ignore') \
                     .sort_values('MEDIAN', ascending=True)
df_thresholds = pd.concat([
    df_thresholds.loc[first_present],
    others
])

# === Сохранение в Excel с форматированием ===
output_path = Path(r"C:\Users\777\Desktop\thresholds.xlsx")
with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
    df_thresholds.to_excel(writer, sheet_name='Лист1')
    workbook  = writer.book
    worksheet = writer.sheets['Лист1']

    # Ширины колонок
    worksheet.set_column('A:A', 21.43)
    worksheet.set_column('B:B', 9.14)
    worksheet.set_column('C:D', 13.00)
    worksheet.set_column('E:E', 13.00)
    worksheet.set_row(0, 16.5)

    # Форматы заголовков
    header_ticker = workbook.add_format({
        'font_name': 'Times New Roman', 'font_size': 12, 'bold': True, 'font_color': '#FF0000',
        'bottom': 2, 'top': 2, 'left': 2, 'right': 1,
    })
    header_q1      = workbook.add_format({'font_name':'Times New Roman','font_size':12,'font_color':'#9C5700','bg_color':'#FFEB9C','bottom':2,'top':2,'left':1,'right':1})
    header_median  = workbook.add_format({'font_name':'Times New Roman','font_size':12,'font_color':'#006100','bg_color':'#C6EFCE','bottom':2,'top':2,'left':1,'right':1})
    header_q3      = workbook.add_format({'font_name':'Times New Roman','font_size':12,'font_color':'#C00000','bg_color':'#FFC7CE','bottom':2,'top':2,'left':1,'right':1})
    header_q90     = workbook.add_format({'font_name':'Times New Roman','font_size':12,'font_color':'#000000','bg_color':'#4BACC6','bottom':2,'top':2,'left':1,'right':2})

    # Форматы данных
    data_idx = workbook.add_format({'font_name':'Times New Roman','font_size':12,'bottom':1,'left':1,'right':1})
    data_q1  = workbook.add_format({'font_name':'Times New Roman','font_size':12,'font_color':'#9C5700','bg_color':'#FFEB9C','bottom':1,'left':1,'right':1})
    data_med = workbook.add_format({'font_name':'Times New Roman','font_size':12,'font_color':'#006100','bg_color':'#C6EFCE','bottom':1,'left':1,'right':1})
    data_q3  = workbook.add_format({'font_name':'Times New Roman','font_size':12,'font_color':'#C00000','bg_color':'#FFC7CE','bottom':1,'left':1,'right':1})
    data_q90 = workbook.add_format({'font_name':'Times New Roman','font_size':12,'font_color':'#000000','bg_color':'#4BACC6','bottom':1,'left':1,'right':1})

    worksheet.set_column('A:A', None, data_idx)
    worksheet.set_column('B:B', None, data_q1)
    worksheet.set_column('C:C', None, data_med)
    worksheet.set_column('D:D', None, data_q3)
    worksheet.set_column('E:E', None, data_q90)

    # Перезапись шапки
    worksheet.write(0, 0, df_thresholds.index.name or 'ticker', header_ticker)
    for col_idx, col_name in enumerate(df_thresholds.columns, start=1):
        fmt = {
            'Q1': header_q1,
            'MEDIAN': header_median,
            'Q3': header_q3,
            'Q90': header_q90
        }[col_name]
        worksheet.write(0, col_idx, col_name, fmt)

print(f"Results saved to {output_path}")
