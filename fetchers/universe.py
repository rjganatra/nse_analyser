"""
fetchers/universe.py
Fetches Nifty 500 from NSE official CSV.
Includes a sector fallback map for stocks that NSE labels poorly.
"""

import csv
import io
import json
import time
from pathlib import Path
import requests

ROOT       = Path(__file__).parent.parent
CACHE_FILE = ROOT / "results" / "universe.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
    "Referer":    "https://www.nseindia.com/",
    "Accept-Language": "en-US,en;q=0.9",
}

NSE_CSV_URL = "https://archives.nseindia.com/content/indices/ind_nifty500list.csv"

# Sector overrides for stocks NSE labels incorrectly or leaves blank
SECTOR_OVERRIDES = {
    # Defence & Aerospace
    "HAL":        "Aerospace & Defence",
    "BEL":        "Aerospace & Defence",
    "BEML":       "Aerospace & Defence",
    "BDL":        "Aerospace & Defence",
    "GRSE":       "Aerospace & Defence",
    "COCHINSHIP": "Aerospace & Defence",
    "MAZDOCK":    "Aerospace & Defence",
    "MIDHANI":    "Aerospace & Defence",
    "DATAPATTNS": "Aerospace & Defence",
    # PSU Banks
    "SBIN":       "Banking",
    "BANKBARODA": "Banking",
    "CANBK":      "Banking",
    "UNIONBANK":  "Banking",
    "INDIANB":    "Banking",
    "BANKINDIA":  "Banking",
    "IOB":        "Banking",
    "CENTRALBK":  "Banking",
    "UCOBANK":    "Banking",
    "MAHABANK":   "Banking",
    # IT
    "TCS":        "IT",
    "INFY":       "IT",
    "WIPRO":      "IT",
    "HCLTECH":    "IT",
    "TECHM":      "IT",
    "LTTS":       "IT",
    "MPHASIS":    "IT",
    "COFORGE":    "IT",
    "PERSISTENT": "IT",
    "OFSS":       "IT",
    # Pharma
    "SUNPHARMA":  "Pharma",
    "DRREDDY":    "Pharma",
    "CIPLA":      "Pharma",
    "DIVISLAB":   "Pharma",
    "LUPIN":      "Pharma",
    "AUROPHARMA": "Pharma",
    "ALKEM":      "Pharma",
    "IPCALAB":    "Pharma",
    # Power
    "NTPC":       "Power",
    "POWERGRID":  "Power",
    "NHPC":       "Power",
    "SJVN":       "Power",
    "TATAPOWER":  "Power",
    "ADANIPOWER": "Power",
    "TORNTPOWER": "Power",
    "CESC":       "Power",
    "JPPOWER":    "Power",
    # Oil & Gas
    "RELIANCE":   "Oil & Gas",
    "ONGC":       "Oil & Gas",
    "IOC":        "Oil & Gas",
    "BPCL":       "Oil & Gas",
    "HINDPETRO":  "Oil & Gas",
    "GAIL":       "Oil & Gas",
    "OIL":        "Oil & Gas",
    "PETRONET":   "Oil & Gas",
    "MGL":        "Oil & Gas",
    "IGL":        "Oil & Gas",
}

NIFTY50_FALLBACK = [
    "RELIANCE","TCS","HDFCBANK","BHARTIARTL","ICICIBANK","INFOSYS","SBIN",
    "HINDUNILVR","ITC","LT","KOTAKBANK","AXISBANK","WIPRO","HCLTECH",
    "BAJFINANCE","MARUTI","TITAN","ULTRACEMCO","ASIANPAINT","NESTLEIND",
]


def fetch_nifty500() -> list[dict]:
    session = requests.Session()
    try:
        session.get("https://www.nseindia.com", headers=HEADERS, timeout=15)
        time.sleep(2)
        r = session.get(NSE_CSV_URL, headers=HEADERS, timeout=20)
        if r.status_code != 200:
            raise Exception(f"HTTP {r.status_code}")

        reader = csv.DictReader(io.StringIO(r.text))
        stocks = []
        for row in reader:
            sym    = row.get("Symbol", "").strip()
            name   = row.get("Company Name", "").strip()
            sector = row.get("Industry", row.get("Sector", "")).strip()

            if not sym:
                continue

            # Apply overrides for known mis-labelled or empty sectors
            sector = SECTOR_OVERRIDES.get(sym, sector) or "Others"

            stocks.append({"symbol": sym, "name": name, "sector": sector})

        if len(stocks) < 100:
            raise Exception(f"Only got {len(stocks)} — suspicious")

        print(f"  [universe] Fetched {len(stocks)} stocks from NSE Nifty 500")
        CACHE_FILE.parent.mkdir(exist_ok=True)
        with open(CACHE_FILE, "w") as f:
            json.dump({"stocks": stocks}, f)
        return stocks

    except Exception as e:
        print(f"  [universe] NSE fetch failed: {e} — trying cache...")
        if CACHE_FILE.exists():
            with open(CACHE_FILE) as f:
                data = json.load(f)
            stocks = data.get("stocks", [])
            if stocks:
                print(f"  [universe] Using cached: {len(stocks)} stocks")
                return stocks

        print(f"  [universe] Using hardcoded fallback")
        return [{"symbol": s, "name": s, "sector": "Others"} for s in NIFTY50_FALLBACK]


def get_symbols() -> list[str]:
    return [s["symbol"] for s in fetch_nifty500()]
