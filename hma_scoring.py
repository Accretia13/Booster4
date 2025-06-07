import asyncio
from okx_downloader import download_and_save_all_candles

tickers = asyncio.run(download_and_save_all_candles(vol_threshold=30.0))
print(len(tickers))
