#!/usr/bin/env python3
"""
scan_opportunities.py
Strategy 2: Daily opportunity scanner.
Uses yfinance .info to get 52W high/low directly — one call per stock, no history download.
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from fetchers.universe import fetch_nifty500
from fetchers.prices import fetch_bulk_prices

ROOT     = Path(__file__).parent
DOCS_DIR = ROOT / "docs"
DOCS_DIR.mkdir(exist_ok=True)
(ROOT / "results").mkdir(exist_ok=True)


def classify(stock: dict) -> list[str]:
    tags     = []
    pct_low  = stock.get("pct_above_52w_low")
    pct_high = stock.get("pct_below_52w_high")
    chg_1m   = stock.get("change_1m_pct")
    chg_3m   = stock.get("change_3m_pct")

    if pct_low is not None:
        if pct_low <= 10:
            tags.append("AT_52W_LOW")
        elif pct_low <= 20:
            tags.append("NEAR_52W_LOW")
        elif pct_low <= 30:
            tags.append("DISCOUNTED_ZONE")

    if pct_high is not None and pct_high <= 5:
        tags.append("NEAR_52W_HIGH")

    if chg_1m is not None:
        if chg_1m >= 40:
            tags.append("STRONG_RECOVERY_1M")
        elif chg_1m >= 20:
            tags.append("RECOVERY_1M")
        elif chg_1m <= -20:
            tags.append("HEAVY_FALL_1M")

    if chg_3m is not None:
        if chg_3m >= 50:
            tags.append("STRONG_RECOVERY_3M")
        elif chg_3m <= -30:
            tags.append("HEAVY_FALL_3M")

    return tags


def run():
    print(f"\n{'='*60}")
    print(f"  STRATEGY 2: OPPORTUNITY SCAN")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}")

    universe = fetch_nifty500()
    symbols  = [s["symbol"] for s in universe]
    meta_map = {s["symbol"]: s for s in universe}

    print(f"\nFetching 52W data for {len(symbols)} stocks...\n")

    prices = fetch_bulk_prices(symbols, delay=1.5)

    all_stocks    = []
    opportunities = []

    for symbol in symbols:
        p        = prices.get(symbol, {})
        nse_info = meta_map.get(symbol, {})
        tags     = classify(p)

        entry = {
            "symbol":             symbol,
            "name":               nse_info.get("name", symbol),
            "sector":             nse_info.get("sector", ""),
            "current_price":      p.get("current_price"),
            "week52_high":        p.get("week52_high"),
            "week52_low":         p.get("week52_low"),
            "pct_above_52w_low":  p.get("pct_above_52w_low"),
            "pct_below_52w_high": p.get("pct_below_52w_high"),
            "change_1m_pct":      p.get("change_1m_pct"),
            "change_3m_pct":      p.get("change_3m_pct"),
            "near_52w_low":       p.get("near_52w_low", False),
            "tags":               tags,
            "is_opportunity":     len(tags) > 0,
        }
        all_stocks.append(entry)
        if tags:
            opportunities.append(entry)

    priority = {
        "AT_52W_LOW": 0, "NEAR_52W_LOW": 1, "DISCOUNTED_ZONE": 2,
        "STRONG_RECOVERY_1M": 3, "RECOVERY_1M": 4, "HEAVY_FALL_1M": 5
    }
    opportunities.sort(
        key=lambda x: min((priority.get(t, 9) for t in x["tags"]), default=9)
    )
    all_stocks.sort(key=lambda x: (x.get("pct_above_52w_low") or 999))

    payload = {
        "meta": {
            "strategy":      "opportunities",
            "scanned_at":    datetime.now(timezone.utc).isoformat(),
            "total":         len(symbols),
            "success":       sum(1 for s in all_stocks if s["current_price"]),
            "opportunities": len(opportunities),
            "data_source":   "yfinance (52W high/low from info)",
        },
        "opportunities": opportunities,
        "all_stocks":    all_stocks,
    }

    for path in [ROOT / "results" / "opportunities.json",
                 DOCS_DIR / "opportunities.json"]:
        with open(path, "w") as f:
            json.dump(payload, f, indent=2, default=str)

    near_low = [s for s in opportunities
                if any(t in s["tags"]
                       for t in ["AT_52W_LOW", "NEAR_52W_LOW", "DISCOUNTED_ZONE"])]

    print(f"\n{'='*60}")
    print(f"  DONE: {payload['meta']['success']}/{len(symbols)} priced")
    print(f"  Opportunities found: {len(opportunities)}")
    print(f"  Near 52W low: {len(near_low)}")
    if near_low:
        print(f"\n  Top 10:")
        for s in near_low[:10]:
            print(f"    {s['symbol']:15} +{s['pct_above_52w_low']}% from 52W low | "
                  f"Tags: {s['tags']}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    run()
