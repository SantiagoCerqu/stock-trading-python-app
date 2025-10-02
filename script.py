#!/Users/santiagocc/Documents/Learnig_Programming/DataExpert/beginner_data_engineering_boot_camp/stock-trading-python-app/.venv/bin/python3.11

import requests
import os
import csv
from dotenv import load_dotenv
load_dotenv()

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

LIMIT = 1000

OUTPUT_CSV = "tickers.csv"

def run_stock_job():

    url = f"https://api.polygon.io/v3/reference/tickers?&market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}"
    response = requests.get(url)

    tickers = []

    data = response.json()

    # 1. Reading data from API

    # First api page
    for ticker in data["results"]:
        tickers.append(ticker)

    # The rest of the pages (next_url)
    while 'next_url' in data:
        response = requests.get(data["next_url"] + f"&apiKey={POLYGON_API_KEY}")
        data = response.json()
        print (data.keys())
        print(data["status"])
        if "error" in data.keys():
            break
        for ticker in data["results"]:
            tickers.append(ticker)


    # 2. Saving data to CSV

    """

        "active": true,
        "cik": "0001090872",
        "composite_figi": "BBG000BWQYZ5",
        "currency_name": "usd",
        "last_updated_utc": "2021-04-25T00:00:00Z",
        "locale": "us",
        "market": "stocks",
        "name": "Agilent Technologies Inc.",
        "primary_exchange": "XNYS",
        "share_class_figi": "BBG001SCTQY4",
        "ticker": "A",
        "type": "CS"

    """
    HEADERS = [
        "active",
        "cik",
        "composite_figi",
        "currency_name",
        "last_updated_utc",
        "locale",
        "market",
        "name",
        "primary_exchange",
        "share_class_figi",
        "ticker",
        "type"
    ]

    with open(OUTPUT_CSV, mode='w', newline='', encoding='utf-8') as File:
        writer = csv.DictWriter(File, fieldnames=HEADERS)
        writer.writeheader()
        for ticker in tickers:
            row = {row:ticker.get(row) for row in HEADERS}

            # print(row)
            writer.writerow(row)

    print(f"Wrote {len(tickers)} rows to {OUTPUT_CSV}")


if __name__ == "__main__":
    run_stock_job()