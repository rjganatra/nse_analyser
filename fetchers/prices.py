"""
fetchers/prices.py
Fetches prices + 52W high/low for ALL Nifty 500 stocks in ONE API call
using NSE's official equity-stockIndices endpoint.
No rate limiting possible - single request, returns all stocks at once.
"""

import json
import time
from pathlib import Path
import requests

ROOT        = Path(__file__).parent.parent
PRICE_CACHE = ROOT / "results" / "prices.json"

HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
    "Referer":         "https://www.nseindia.com/market-data/live-equity-market",
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
    "X-Requested-With":"XMLHttpRequest",
}

NSE_INDICES = [
    "NIFTY 500",
    "NIFTY MIDCAP 150",
    "NIFTY SMALLCAP 250",
]


def _get_nse_session() -> requests.Session:
    """Visit NSE homepage to get required session cookies."""
    session = requests.Session()
    try:
        # Step 1: get cookies from homepage
        session.get(
            "https://www.nseindia.com",
            headers={**HEADERS, "Accept": "text/html"},
            timeout=15
        )
        time.sleep(2)
        # Step 2: visit market data page to get more cookies
        session.get(
            "https://www.nseindia.com/market-data/live-equity-market",
            headers={**HEADERS, "Accept": "text/html"},
            timeout=15
        )
        time.sleep(2)
    except Exception as e:
        print(f"  [prices] Session setup warning: {e}")
    return session


def _fetch_index(session: requests.Session, index_name: str) -> list[dict]:
    """Fetch all stocks in an NSE index with price + 52W data."""
    import urllib.parse
    url = f"https://www.nseindia.com/api/equity-stockIndices?index={urllib.parse.quote(index_name)}"
    try:
        r = session.get(url, headers=HEADERS, timeout=20)
        if r.status_code != 200:
            print(f"  [prices] {index_name}: HTTP {r.status_code}")
            return []
        data = r.json()
        stocks = data.get("data", [])
        # Remove the index row itself (first row is usually the index summary)
        stocks = [s for s in stocks if s.get("symbol") and s.get("symbol") != index_name]
        return stocks
    except Exception as e:
        print(f"  [prices] {index_name} error: {e}")
        return []


def fetch_bulk_prices(symbols: list[str], delay: float = 1.0) -> dict:
    """
    Fetch 52W high/low + current price for all symbols.
    Uses NSE equity-stockIndices API — single call per index, covers all 500 stocks.
    """
    print(f"  [prices] Setting up NSE session...")
    session = _get_nse_session()

    all_nse_data = {}

    for index_name in NSE_INDICES:
        print(f"  [prices] Fetching {index_name}...")
        stocks = _fetch_index(session, index_name)
        for s in stocks:
            sym = s.get("symbol", "").strip()
            if sym:
                all_nse_data[sym] = s
        time.sleep(1)

    print(f"  [prices] NSE returned data for {len(all_nse_data)} stocks")

    results = {}
    for symbol in symbols:
        s = all_nse_data.get(symbol)
        if not s:
            results[symbol] = _empty(symbol, "not in NSE index data")
            continue

        def safe(key) -> float | None:
            try:
                v = s.get(key)
                if v is None or v == "" or v == "-":
                    return None
                return float(str(v).replace(",", ""))
            except Exception:
                return None

        current  = safe("lastPrice")
        w52_high = safe("yearHigh") or safe("weekHigh52")
        w52_low  = safe("yearLow")  or safe("weekLow52")

        pct_above = None
        pct_below = None
        if current and w52_low and w52_low > 0:
            pct_above = round(((current - w52_low) / w52_low) * 100, 1)
        if current and w52_high and w52_high > 0:
            pct_below = round(((w52_high - current) / w52_high) * 100, 1)

        # 1M change from pChange (NSE provides % change for different periods)
        chg_1m = safe("perChange365d")   # 1 year change — use as proxy
        chg_1d = safe("pChange")          # 1 day change
        # NSE provides perChange30d in some responses
        chg_30d = safe("perChange30d") or safe("change30d")

        results[symbol] = {
            "symbol":             symbol,
            "current_price":      current,
            "week52_high":        w52_high,
            "week52_low":         w52_low,
            "pct_above_52w_low":  pct_above,
            "pct_below_52w_high": pct_below,
            "near_52w_low":       pct_above is not None and pct_above <= 30,
            "change_1d_pct":      chg_1d,
            "change_1m_pct":      chg_30d,
            "change_1y_pct":      chg_1m,
        }

    ok = sum(1 for v in results.values() if v.get("current_price"))
    print(f"  [prices] Done: {ok}/{len(symbols)} stocks fetched")

    PRICE_CACHE.parent.mkdir(exist_ok=True)
    with open(PRICE_CACHE, "w") as f:
        json.dump(results, f, indent=2)

    return results


def _empty(symbol: str, error: str = "") -> dict:
    return {
        "symbol": symbol, "current_price": None,
        "week52_high": None, "week52_low": None,
        "pct_above_52w_low": None, "pct_below_52w_high": None,
        "near_52w_low": False, "change_1m_pct": None,
        "change_1y_pct": None, "error": error,
    }


def load_cached_prices() -> dict:
    if PRICE_CACHE.exists():
        with open(PRICE_CACHE) as f:
            return json.load(f)
    return {}
