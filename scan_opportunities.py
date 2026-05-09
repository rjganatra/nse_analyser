#!/usr/bin/env python3
"""
scan_opportunities.py - Daily. Fetches price, 52W, all ratios from Screener.in.
Runs at 12:00 PM IST and 5:00 PM IST on weekdays.
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from fetchers.universe import fetch_nifty500
from fetchers.screener import fetch

ROOT     = Path(__file__).parent
DOCS_DIR = ROOT / "docs"
DOCS_DIR.mkdir(exist_ok=True)
(ROOT / "results").mkdir(exist_ok=True)


def classify(stock: dict) -> list[str]:
    tags     = []
    pct_low  = stock.get("pct_above_52w_low")
    pct_high = stock.get("pct_below_52w_high")
    if pct_low is not None:
        if pct_low <= 10:   tags.append("AT_52W_LOW")
        elif pct_low <= 20: tags.append("NEAR_52W_LOW")
        elif pct_low <= 30: tags.append("DISCOUNTED_ZONE")
    if pct_high is not None and pct_high <= 5:
        tags.append("NEAR_52W_HIGH")
    return tags


def run():
    print(f"\n{'='*60}")
    print(f"  STRATEGY 2: OPPORTUNITY SCAN")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}")

    universe = fetch_nifty500()
    symbols  = [s["symbol"] for s in universe]
    meta_map = {s["symbol"]: s for s in universe}
    total    = len(symbols)

    print(f"\nFetching data for {total} stocks from Screener.in...\n")

    all_stocks = []
    errors     = []

    for i, symbol in enumerate(symbols, 1):
        print(f"[{i}/{total}] {symbol}", end=" ", flush=True)
        try:
            nse_info = meta_map.get(symbol, {})
            # Pass NSE sector directly — reliable, avoids Screener HTML parsing issues
            nse_sector = nse_info.get("sector", "")

            raw = fetch(symbol, delay=2.0, nse_sector=nse_sector)

            if "error" in raw:
                print(f"-- ERROR: {raw['error']}")
                errors.append(symbol)
                continue

            pd   = raw.get("price_data", {})
            top  = raw.get("top_ratios", {})
            tags = classify(pd)

            entry = {
                "symbol":             symbol,
                "name":               raw.get("name") or nse_info.get("name", symbol),
                "sector":             nse_sector,   # always use NSE sector
                "current_price":      pd.get("current_price"),
                "week52_high":        pd.get("week52_high"),
                "week52_low":         pd.get("week52_low"),
                "pct_above_52w_low":  pd.get("pct_above_52w_low"),
                "pct_below_52w_high": pd.get("pct_below_52w_high"),
                "near_52w_low":       pd.get("near_52w_low", False),
                "tags":               tags,
                "is_opportunity":     len(tags) > 0,
                "screener_url":       raw.get("url", ""),
                "ratios": {
                    "pe":          top.get("P/E"),
                    "industry_pe": top.get("Industry P/E"),
                    "pb":          top.get("P/B"),
                    "roe":         top.get("ROE %"),
                    "roce":        top.get("ROCE %"),
                    "de":          top.get("Debt / Equity"),
                    "div_yield":   top.get("Div. Yield %"),
                    "market_cap":  top.get("Market Cap"),
                },
            }

            print(f"-- ₹{pd.get('current_price')} | "
                  f"52W:{pd.get('week52_low')}-{pd.get('week52_high')} | "
                  f"PE={top.get('P/E')} IndPE={top.get('Industry P/E')} "
                  f"ROE={top.get('ROE %')} | tags={tags or 'none'}")

            all_stocks.append(entry)

        except Exception as e:
            print(f"-- EXCEPTION: {e}")
            errors.append(symbol)

    opportunities = [s for s in all_stocks if s["is_opportunity"]]
    priority = {"AT_52W_LOW": 0, "NEAR_52W_LOW": 1, "DISCOUNTED_ZONE": 2, "NEAR_52W_HIGH": 3}
    opportunities.sort(key=lambda x: min((priority.get(t,9) for t in x["tags"]), default=9))
    all_stocks.sort(key=lambda x: (x.get("pct_above_52w_low") or 999))

    priced  = sum(1 for s in all_stocks if s["current_price"])
    payload = {
        "meta": {
            "strategy":      "opportunities",
            "scanned_at":    datetime.now(timezone.utc).isoformat(),
            "total":         total,
            "success":       priced,
            "opportunities": len(opportunities),
            "errors":        len(errors),
            "data_source":   "Screener.in",
        },
        "opportunities": opportunities,
        "all_stocks":    all_stocks,
    }

    for path in [ROOT/"results"/"opportunities.json", DOCS_DIR/"opportunities.json"]:
        with open(path, "w") as f:
            json.dump(payload, f, indent=2, default=str)

    near_low = [s for s in opportunities
                if any(t in s["tags"] for t in ["AT_52W_LOW","NEAR_52W_LOW","DISCOUNTED_ZONE"])]
    print(f"\n{'='*60}")
    print(f"  DONE: {priced}/{total} priced | {len(errors)} errors")
    print(f"  Near 52W low: {len(near_low)}")
    for s in near_low[:5]:
        print(f"    {s['symbol']:12} ₹{s['current_price']} | "
              f"52W low ₹{s['week52_low']} | +{s['pct_above_52w_low']}% | "
              f"PE={s['ratios'].get('pe')} IndPE={s['ratios'].get('industry_pe')}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    run()
