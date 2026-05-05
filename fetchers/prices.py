"""
fetchers/prices.py
Fetches 52W price data using yfinance — individual tickers, threaded.
"""

import json
import time
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import yfinance as yf

ROOT = Path(__file__).parent.parent
PRICE_CACHE = ROOT / "results" / "prices.json"


def _nse(symbol: str) -> str:
    special = {"M&M": "M%26M.NS", "M&MFIN": "M%26MFIN.NS",
               "J&KBANK": "J%26KBANK.NS", "ARE&M": "ARE%26M.NS", "GVT&D": "GVT%26D.NS"}
    return special.get(symbol, f"{symbol}.NS")


def _fetch_one(symbol: str) -> dict:
    for attempt in range(3):
        try:
            hist = yf.Ticker(_nse(symbol)).history(period="1y", auto_adjust=True)
            if hist.empty:
                return _empty(symbol, "no data")

            close = hist["Close"].dropna()
            if len(close) < 2:
                return _empty(symbol, "insufficient history")

            current  = float(close.iloc[-1])
            w52_high = float(close.max())
            w52_low  = float(close.min())

            pct_above = round(((current - w52_low) / w52_low) * 100, 1) if w52_low > 0 else None
            pct_below = round(((w52_high - current) / w52_high) * 100, 1) if w52_high > 0 else None
            chg_1m = round(((current - float(close.iloc[-22])) / float(close.iloc[-22])) * 100, 1) if len(close) >= 22 else None
            chg_3m = round(((current - float(close.iloc[-66])) / float(close.iloc[-66])) * 100, 1) if len(close) >= 66 else None

            return {
                "symbol": symbol, "current_price": round(current, 2),
                "week52_high": round(w52_high, 2), "week52_low": round(w52_low, 2),
                "pct_above_52w_low": pct_above, "pct_below_52w_high": pct_below,
                "near_52w_low": pct_above is not None and pct_above <= 30,
                "change_1m_pct": chg_1m, "change_3m_pct": chg_3m,
            }
        except Exception as e:
            if "rate" in str(e).lower() or "429" in str(e):
                time.sleep((attempt + 1) * 10)
            else:
                return _empty(symbol, str(e))
    return _empty(symbol, "max retries")


def _empty(symbol, error=""):
    return {"symbol": symbol, "current_price": None, "week52_high": None,
            "week52_low": None, "pct_above_52w_low": None, "pct_below_52w_high": None,
            "near_52w_low": False, "change_1m_pct": None, "change_3m_pct": None, "error": error}


def fetch_bulk_prices(symbols: list[str], max_workers: int = 5) -> dict:
    """Fetch prices for all symbols using thread pool. max_workers=5 avoids rate limits."""
    print(f"  [prices] Fetching {len(symbols)} stocks (5 threads)...")
    results = {}
    done = 0
    lock = threading.Lock()

    def fetch_and_track(symbol):
        nonlocal done
        result = _fetch_one(symbol)
        time.sleep(0.3)
        with lock:
            done += 1
            if done % 100 == 0:
                ok = sum(1 for v in results.values() if v.get("current_price"))
                print(f"  [prices] {done}/{len(symbols)} done, {ok} successful")
        return symbol, result

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_and_track, s): s for s in symbols}
        for f in as_completed(futures):
            sym, res = f.result()
            results[sym] = res

    ok = sum(1 for v in results.values() if v.get("current_price"))
    print(f"  [prices] Done: {ok}/{len(symbols)} successful")

    PRICE_CACHE.parent.mkdir(exist_ok=True)
    with open(PRICE_CACHE, "w") as f:
        json.dump(results, f)
    return results


def load_cached_prices() -> dict:
    if PRICE_CACHE.exists():
        with open(PRICE_CACHE) as f:
            return json.load(f)
    return {}
