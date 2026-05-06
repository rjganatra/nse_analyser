"""
fetchers/prices.py
Gets 52W high, 52W low, current price, 1M/3M change from yfinance .info
One API call per stock. Single threaded with delay. No rate limiting.
"""

import time
import json
from pathlib import Path
import yfinance as yf

ROOT        = Path(__file__).parent.parent
PRICE_CACHE = ROOT / "results" / "prices.json"

SPECIAL = {
    "M&M": "M%26M.NS", "M&MFIN": "M%26MFIN.NS",
    "J&KBANK": "J%26KBANK.NS", "ARE&M": "ARE%26M.NS", "GVT&D": "GVT%26D.NS"
}


def _nse(symbol: str) -> str:
    return SPECIAL.get(symbol, f"{symbol}.NS")


def _safe(val) -> float | None:
    try:
        import math
        f = float(val)
        return None if math.isnan(f) or math.isinf(f) else f
    except Exception:
        return None


def fetch_bulk_prices(symbols: list[str], delay: float = 1.5) -> dict:
    """
    Fetches price data for all symbols one by one using yfinance .info
    which returns 52W high/low directly without downloading history.
    Single threaded with delay to avoid rate limiting.
    """
    results = {}
    total   = len(symbols)
    print(f"  [prices] Fetching {total} stocks via yfinance info (1 call each)...")

    for i, symbol in enumerate(symbols, 1):
        if i % 50 == 0:
            ok = sum(1 for v in results.values() if v.get("current_price"))
            print(f"  [prices] {i}/{total} done, {ok} successful")

        for attempt in range(3):
            try:
                info = yf.Ticker(_nse(symbol)).info

                current  = _safe(info.get("currentPrice") or info.get("regularMarketPrice"))
                w52_high = _safe(info.get("fiftyTwoWeekHigh"))
                w52_low  = _safe(info.get("fiftyTwoWeekLow"))

                # 1M and 3M change using available price points
                prev_1m  = _safe(info.get("regularMarketPreviousClose"))  # fallback
                chg_1m   = None
                chg_3m   = None

                if current and w52_low and w52_high:
                    # Use 50-day and 200-day avg as proxies when history not available
                    avg_50  = _safe(info.get("fiftyDayAverage"))
                    avg_200 = _safe(info.get("twoHundredDayAverage"))
                    if current and avg_50 and avg_50 > 0:
                        chg_1m = round(((current - avg_50) / avg_50) * 100, 1)
                    if current and avg_200 and avg_200 > 0:
                        chg_3m = round(((current - avg_200) / avg_200) * 100, 1)

                pct_above = None
                pct_below = None
                if current and w52_low and w52_low > 0:
                    pct_above = round(((current - w52_low) / w52_low) * 100, 1)
                if current and w52_high and w52_high > 0:
                    pct_below = round(((w52_high - current) / w52_high) * 100, 1)

                results[symbol] = {
                    "symbol":             symbol,
                    "current_price":      current,
                    "week52_high":        w52_high,
                    "week52_low":         w52_low,
                    "pct_above_52w_low":  pct_above,
                    "pct_below_52w_high": pct_below,
                    "near_52w_low":       pct_above is not None and pct_above <= 30,
                    "change_1m_pct":      chg_1m,
                    "change_3m_pct":      chg_3m,
                }
                time.sleep(delay)
                break

            except Exception as e:
                err = str(e)
                if "rate" in err.lower() or "429" in err:
                    wait = (attempt + 1) * 15
                    print(f"    [{symbol}] rate limited, waiting {wait}s...")
                    time.sleep(wait)
                else:
                    results[symbol] = _empty(symbol, err)
                    break
        else:
            results[symbol] = _empty(symbol, "max retries")

    ok = sum(1 for v in results.values() if v.get("current_price"))
    print(f"  [prices] Done: {ok}/{total} successful")

    PRICE_CACHE.parent.mkdir(exist_ok=True)
    with open(PRICE_CACHE, "w") as f:
        json.dump(results, f)
    return results


def _empty(symbol: str, error: str = "") -> dict:
    return {
        "symbol": symbol, "current_price": None, "week52_high": None,
        "week52_low": None, "pct_above_52w_low": None, "pct_below_52w_high": None,
        "near_52w_low": False, "change_1m_pct": None, "change_3m_pct": None,
        "error": error,
    }


def load_cached_prices() -> dict:
    if PRICE_CACHE.exists():
        with open(PRICE_CACHE) as f:
            return json.load(f)
    return {}
