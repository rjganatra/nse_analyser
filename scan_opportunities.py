#!/usr/bin/env python3
"""
scan_opportunities.py
Strategy 2: Daily opportunity scanner for all Nifty 500 stocks.
Uses yfinance to bulk-fetch price data. Fast — runs in ~5 minutes.
Flags stocks:
  - Near 52W low (within 30%) -- discounted opportunity
  - Strong 1-month recovery (>20% up) -- post-event bounce
  - Near 52W high (within 5%) -- momentum
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from fetchers.universe import fetch_nifty500
from fetchers.prices import fetch_bulk_prices

ROOT = Path(__file__).parent
DOCS_DIR = ROOT / "docs"
DOCS_DIR.mkdir(exist_ok=True)


def classify(stock: dict) -> list[str]:
    """Return list of opportunity tags for a stock."""
    tags = []
    pct_low  = stock.get("pct_above_52w_low")
    pct_high = stock.get("pct_below_52w_high")
    chg_1m   = stock.get("change_1m_pct")
    chg_3m   = stock.get("change_3m_pct")

    if pct_low is not None:
        if pct_low <= 10:   tags.append("AT_52W_LOW")
        elif pct_low <= 20: tags.append("NEAR_52W_LOW")
        elif pct_low <= 30: tags.append("DISCOUNTED_ZONE")

    if pct_high is not None and pct_high <= 5:
        tags.append("NEAR_52W_HIGH")

    if chg_1m is not None:
        if chg_1m >= 40:  tags.append("STRONG_RECOVERY_1M")
        elif chg_1m >= 20: tags.append("RECOVERY_1M")
        elif chg_1m <= -20: tags.append("HEAVY_FALL_1M")

    if chg_3m is not None:
        if chg_3m >= 50:   tags.append("STRONG_RECOVERY_3M")
        elif chg_3m <= -30: tags.append("HEAVY_FALL_3M")

    return tags


def run():
    print(f"\n{'='*60}")
    print(f"  STRATEGY 2: OPPORTUNITY SCAN")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}")

    # Step 1: Get universe
    universe = fetch_nifty500()
    symbols  = [s["symbol"] for s in universe]
    meta_map = {s["symbol"]: s for s in universe}

    print(f"\nFetching prices for {len(symbols)} stocks...\n")

    # Step 2: Bulk fetch all prices via yfinance
    prices = fetch_bulk_prices(symbols, max_workers=5)

    # Step 3: Classify each stock
    all_stocks    = []
    opportunities = []
    errors        = []

    for symbol in symbols:
        p = prices.get(symbol, {})
        if p.get("error") and not p.get("current_price"):
            errors.append(symbol)
            continue

        nse_info = meta_map.get(symbol, {})
        tags = classify(p)

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

    # Sort opportunities: AT_52W_LOW first, then NEAR, then others
    priority = {"AT_52W_LOW": 0, "NEAR_52W_LOW": 1, "DISCOUNTED_ZONE": 2,
                "RECOVERY_1M": 3, "STRONG_RECOVERY_1M": 3, "HEAVY_FALL_1M": 4}
    opportunities.sort(key=lambda x: min((priority.get(t, 9) for t in x["tags"]), default=9))

    # Sort all stocks by pct_above_52w_low ascending (closest to 52W low first)
    all_stocks.sort(key=lambda x: (x.get("pct_above_52w_low") or 999))

    now = datetime.now(timezone.utc).isoformat()

    payload = {
        "meta": {
            "strategy":        "opportunities",
            "scanned_at":      now,
            "total":           len(symbols),
            "success":         len(all_stocks),
            "opportunities":   len(opportunities),
            "errors":          len(errors),
        },
        "opportunities": opportunities,
        "all_stocks":    all_stocks,
    }

    # Write output files
    for path in [ROOT / "results" / "opportunities.json",
                 ROOT / "docs" / "opportunities.json"]:
        path.parent.mkdir(exist_ok=True)
        with open(path, "w") as f:
            json.dump(payload, f, indent=2, default=str)

    print(f"\n{'='*60}")
    print(f"  DONE: {len(all_stocks)}/{len(symbols)} stocks priced")
    print(f"  Opportunities found: {len(opportunities)}")
    print(f"\n  Near 52W low (<=30%):")
    near_low = [s for s in opportunities if any(t in s["tags"] for t in ["AT_52W_LOW","NEAR_52W_LOW","DISCOUNTED_ZONE"])]
    for s in near_low[:10]:
        print(f"    {s['symbol']:15} {s['pct_above_52w_low']:+.1f}% above 52W low | 1M: {s['change_1m_pct'] or 'N/A'}%  Tags: {s['tags']}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    run()
