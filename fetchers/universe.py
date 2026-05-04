"""
fetchers/universe.py
Fetches Nifty 500 constituent list from NSE public CSV.
NSE updates this file automatically on reconstitutions, mergers, demergers.
Falls back to a hardcoded Nifty 50 list if NSE is unreachable.
"""

import csv
import io
import json
import time
from pathlib import Path
import requests

ROOT = Path(__file__).parent.parent
CACHE_FILE = ROOT / "results" / "universe.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
    "Referer": "https://www.nseindia.com/",
    "Accept-Language": "en-US,en;q=0.9",
}

NSE_CSV_URL = "https://archives.nseindia.com/content/indices/ind_nifty500list.csv"

# Fallback: Nifty 50 hardcoded in case NSE blocks the runner
NIFTY50_FALLBACK = [
    "RELIANCE","TCS","HDFCBANK","BHARTIARTL","ICICIBANK","INFOSYS","SBIN","HINDUNILVR",
    "ITC","LT","KOTAKBANK","AXISBANK","WIPRO","HCLTECH","BAJFINANCE","MARUTI","TITAN",
    "ULTRACEMCO","ASIANPAINT","NESTLEIND","TECHM","SUNPHARMA","POWERGRID","NTPC","ADANIPORTS",
    "ADANIENT","ONGC","JSWSTEEL","TATASTEEL","BAJAJFINSV","GRASIM","INDUSINDBK","CIPLA",
    "DIVISLAB","DRREDDY","EICHERMOT","COALINDIA","HINDALCO","BPCL","HEROMOTOCO","BRITANNIA",
    "TATACONSUM","APOLLOHOSP","HDFCLIFE","SBILIFE","BAJAJ-AUTO","M&M","TATAMOTORS","VEDL","UPL"
]


def fetch_nifty500() -> list[dict]:
    """
    Returns list of dicts: [{"symbol": "TITAN", "name": "Titan Company Ltd", "sector": "Consumer Durables"}, ...]
    """
    session = requests.Session()
    try:
        # NSE requires a valid session cookie first
        session.get("https://www.nseindia.com", headers=HEADERS, timeout=15)
        time.sleep(2)
        r = session.get(NSE_CSV_URL, headers=HEADERS, timeout=20)

        if r.status_code != 200:
            raise Exception(f"HTTP {r.status_code}")

        reader = csv.DictReader(io.StringIO(r.text))
        stocks = []
        for row in reader:
            sym = row.get("Symbol", "").strip()
            name = row.get("Company Name", "").strip()
            sector = row.get("Industry", row.get("Sector", "")).strip()
            if sym:
                stocks.append({"symbol": sym, "name": name, "sector": sector})

        if len(stocks) < 100:
            raise Exception(f"Only got {len(stocks)} symbols — suspicious")

        print(f"  [universe] Fetched {len(stocks)} stocks from NSE Nifty 500")

        # Save cache
        CACHE_FILE.parent.mkdir(exist_ok=True)
        with open(CACHE_FILE, "w") as f:
            json.dump({"stocks": stocks, "count": len(stocks)}, f)

        return stocks

    except Exception as e:
        print(f"  [universe] NSE fetch failed: {e} — trying cache...")

        # Try cache
        if CACHE_FILE.exists():
            with open(CACHE_FILE) as f:
                data = json.load(f)
            stocks = data.get("stocks", [])
            if stocks:
                print(f"  [universe] Using cached universe: {len(stocks)} stocks")
                return stocks

        # Last resort: hardcoded fallback
        print(f"  [universe] Using hardcoded Nifty 50 fallback ({len(NIFTY50_FALLBACK)} stocks)")
        return [{"symbol": s, "name": s, "sector": ""} for s in NIFTY50_FALLBACK]


def get_symbols() -> list[str]:
    return [s["symbol"] for s in fetch_nifty500()]
