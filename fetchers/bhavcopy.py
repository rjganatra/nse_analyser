"""
fetchers/bhavcopy.py
Downloads NSE CM bhavcopy (official daily OHLC CSV) and maintains
a rolling price history to compute 52W high/low without any API.

NSE publishes bhavcopy after 6pm every trading day.
URL: https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_YYYYMMDD_F_0000.csv
No auth required — just needs an nseindia.com session cookie.
"""

import csv
import gzip
import io
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

ROOT         = Path(__file__).parent.parent
HISTORY_FILE = ROOT / "results" / "price_history.json"
RESULTS_DIR  = ROOT / "results"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
    "Referer":    "https://www.nseindia.com/",
    "Accept":     "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Keep 260 trading days (~1 year + buffer)
MAX_HISTORY_DAYS = 260


def _nse_session() -> requests.Session:
    """Get NSE session cookie required for bhavcopy download."""
    s = requests.Session()
    try:
        s.get("https://www.nseindia.com", headers=HEADERS, timeout=15)
        time.sleep(2)
    except Exception:
        pass
    return s


def _bhavcopy_url(date: datetime) -> str:
    return (
        f"https://nsearchives.nseindia.com/content/cm/"
        f"BhavCopy_NSE_CM_0_0_0_{date.strftime('%Y%m%d')}_F_0000.csv"
    )


def _download_bhavcopy(session: requests.Session, date: datetime) -> dict | None:
    """
    Download and parse bhavcopy for given date.
    Returns {symbol: {open, high, low, close, volume}} or None if unavailable.
    """
    url = _bhavcopy_url(date)
    try:
        r = session.get(url, headers=HEADERS, timeout=20)
        if r.status_code != 200:
            return None

        content = r.content
        # Handle gzip if needed
        if url.endswith(".gz") or r.headers.get("Content-Encoding") == "gzip":
            content = gzip.decompress(content)

        text    = content.decode("utf-8", errors="ignore")
        reader  = csv.DictReader(io.StringIO(text))
        result  = {}

        for row in reader:
            # Try both old and new bhavcopy column names
            symbol = (row.get("TckrSymb") or row.get("SYMBOL") or "").strip()
            series = (row.get("SctySrs")  or row.get("SERIES") or "").strip()

            # Only equity (EQ series)
            if not symbol or series not in ("EQ", "BE", "BZ", "SM", "ST"):
                continue

            try:
                result[symbol] = {
                    "o": float(row.get("OpnPric")  or row.get("OPEN")  or 0),
                    "h": float(row.get("HghPric")  or row.get("HIGH")  or 0),
                    "l": float(row.get("LwPric")   or row.get("LOW")   or 0),
                    "c": float(row.get("ClsPric")  or row.get("CLOSE") or
                               row.get("LastPric") or row.get("LAST")  or 0),
                    "v": float(row.get("TtlTradgVol") or row.get("TOTTRDQTY") or 0),
                }
            except (ValueError, TypeError):
                continue

        if result:
            print(f"  [bhavcopy] {date.strftime('%Y-%m-%d')}: {len(result)} stocks")
        return result if result else None

    except Exception as e:
        print(f"  [bhavcopy] Error for {date.strftime('%Y-%m-%d')}: {e}")
        return None


def _load_history() -> dict:
    """Load stored price history. Format: {date_str: {symbol: {o,h,l,c,v}}}"""
    if HISTORY_FILE.exists():
        try:
            with open(HISTORY_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_history(history: dict):
    RESULTS_DIR.mkdir(exist_ok=True)
    # Trim to last MAX_HISTORY_DAYS dates
    dates = sorted(history.keys())
    if len(dates) > MAX_HISTORY_DAYS:
        for old in dates[:-MAX_HISTORY_DAYS]:
            del history[old]
    with open(HISTORY_FILE, "w") as f:
        json.dump(history, f)


def update_and_get_prices(symbols: list[str]) -> dict[str, dict]:
    """
    Main entry point:
    1. Try to download today's + recent missing bhavcopy files
    2. Compute 52W high/low from accumulated history
    3. Return price data for all symbols
    """
    history = _load_history()
    session = _nse_session()
    today   = datetime.now(timezone.utc)

    # Try last 5 days to catch the most recent trading day
    downloaded_any = False
    for days_back in range(0, 5):
        check_date = today - timedelta(days=days_back)
        date_str   = check_date.strftime("%Y-%m-%d")

        # Skip weekends
        if check_date.weekday() >= 5:
            continue

        # Already have this date
        if date_str in history:
            downloaded_any = True
            break

        data = _download_bhavcopy(session, check_date)
        if data:
            history[date_str] = data
            downloaded_any = True
            break
        time.sleep(1)

    if not downloaded_any:
        print("  [bhavcopy] Could not download recent bhavcopy — using cached history")

    _save_history(history)

    # Compute 52W stats for each symbol from history
    dates_sorted = sorted(history.keys())
    latest_date  = dates_sorted[-1] if dates_sorted else None

    results = {}
    for symbol in symbols:
        closes = []
        highs  = []
        lows   = []

        for d in dates_sorted:
            entry = history[d].get(symbol)
            if entry and entry.get("c", 0) > 0:
                closes.append(entry["c"])
                highs.append(entry["h"])
                lows.append(entry["l"])

        if not closes:
            results[symbol] = _empty(symbol)
            continue

        current   = closes[-1]
        w52_high  = max(highs)
        w52_low   = min(lows)

        pct_above = round(((current - w52_low)  / w52_low)  * 100, 1) if w52_low  > 0 else None
        pct_below = round(((w52_high - current) / w52_high) * 100, 1) if w52_high > 0 else None

        chg_1m = None
        if len(closes) >= 22:
            prev = closes[-22]
            chg_1m = round(((current - prev) / prev) * 100, 1) if prev > 0 else None

        chg_3m = None
        if len(closes) >= 66:
            prev = closes[-66]
            chg_3m = round(((current - prev) / prev) * 100, 1) if prev > 0 else None

        results[symbol] = {
            "symbol":             symbol,
            "current_price":      round(current, 2),
            "week52_high":        round(w52_high, 2),
            "week52_low":         round(w52_low, 2),
            "pct_above_52w_low":  pct_above,
            "pct_below_52w_high": pct_below,
            "near_52w_low":       pct_above is not None and pct_above <= 30,
            "change_1m_pct":      chg_1m,
            "change_3m_pct":      chg_3m,
            "days_of_history":    len(closes),
        }

    ok = sum(1 for v in results.values() if v.get("current_price"))
    print(f"  [bhavcopy] Computed prices for {ok}/{len(symbols)} stocks "
          f"({len(dates_sorted)} days of history)")
    return results


def _empty(symbol: str) -> dict:
    return {
        "symbol": symbol, "current_price": None, "week52_high": None,
        "week52_low": None, "pct_above_52w_low": None, "pct_below_52w_high": None,
        "near_52w_low": False, "change_1m_pct": None, "change_3m_pct": None,
        "days_of_history": 0,
    }
