"""
fetchers/prices.py
Fetches live price data for all Nifty 500 stocks using yfinance.
Bulk downloads 52W high, 52W low, current price in one call.
yfinance is free, no API key needed, no rate limits for bulk.
"""

import json
import time
from pathlib import Path
import yfinance as yf
import pandas as pd

ROOT = Path(__file__).parent.parent
PRICE_CACHE = ROOT / "results" / "prices.json"


def _nse_ticker(symbol: str) -> str:
    """Convert NSE symbol to Yahoo Finance ticker format."""
    # Handle special cases
    special = {
        "M&M": "M%26M.NS",
        "M&MFIN": "M%26MFIN.NS",
    }
    if symbol in special:
        return special[symbol]
    return f"{symbol}.NS"


def fetch_bulk_prices(symbols: list[str], chunk_size: int = 100) -> dict[str, dict]:
    """
    Bulk fetch 52W high, 52W low, current price for all symbols.
    Returns {symbol: {price, week52_high, week52_low, pct_above_52w_low, near_52w_low, change_1m_pct}}
    """
    print(f"  [prices] Fetching prices for {len(symbols)} stocks via yfinance...")
    results = {}

    # Split into chunks to avoid timeouts
    chunks = [symbols[i:i+chunk_size] for i in range(0, len(symbols), chunk_size)]

    for idx, chunk in enumerate(chunks):
        tickers = [_nse_ticker(s) for s in chunk]
        ticker_to_sym = {_nse_ticker(s): s for s in chunk}

        print(f"  [prices] Chunk {idx+1}/{len(chunks)} ({len(chunk)} stocks)...")
        try:
            data = yf.download(
                tickers=tickers,
                period="1y",
                interval="1d",
                group_by="ticker",
                auto_adjust=True,
                progress=False,
                threads=True,
            )

            for ticker, symbol in ticker_to_sym.items():
                try:
                    if len(chunk) == 1:
                        df = data
                    else:
                        df = data[ticker] if ticker in data.columns.get_level_values(0) else None

                    if df is None or df.empty:
                        results[symbol] = _empty(symbol)
                        continue

                    df = df.dropna(subset=["Close"])
                    if df.empty:
                        results[symbol] = _empty(symbol)
                        continue

                    close = df["Close"]
                    current = float(close.iloc[-1])
                    w52_high = float(close.max())
                    w52_low  = float(close.min())

                    pct_above_low = round(((current - w52_low) / w52_low) * 100, 1) if w52_low > 0 else None
                    pct_below_high = round(((w52_high - current) / w52_high) * 100, 1) if w52_high > 0 else None
                    near_52w_low = (pct_above_low is not None and pct_above_low <= 30)

                    # 1-month return
                    change_1m = None
                    if len(close) >= 22:
                        price_1m_ago = float(close.iloc[-22])
                        change_1m = round(((current - price_1m_ago) / price_1m_ago) * 100, 1)

                    # 3-month return
                    change_3m = None
                    if len(close) >= 66:
                        price_3m_ago = float(close.iloc[-66])
                        change_3m = round(((current - price_3m_ago) / price_3m_ago) * 100, 1)

                    results[symbol] = {
                        "symbol": symbol,
                        "current_price": round(current, 2),
                        "week52_high": round(w52_high, 2),
                        "week52_low": round(w52_low, 2),
                        "pct_above_52w_low": pct_above_low,
                        "pct_below_52w_high": pct_below_high,
                        "near_52w_low": near_52w_low,
                        "change_1m_pct": change_1m,
                        "change_3m_pct": change_3m,
                    }

                except Exception as e:
                    results[symbol] = _empty(symbol, str(e))

        except Exception as e:
            print(f"  [prices] Chunk {idx+1} failed: {e}")
            for sym in chunk:
                results[sym] = _empty(sym, str(e))

        time.sleep(1)  # polite delay between chunks

    # Save cache
    PRICE_CACHE.parent.mkdir(exist_ok=True)
    with open(PRICE_CACHE, "w") as f:
        json.dump(results, f)

    success = sum(1 for v in results.values() if v.get("current_price"))
    print(f"  [prices] Done: {success}/{len(symbols)} stocks fetched successfully")
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
