#!/usr/bin/env python3
"""
scan_opportunities.py - Uses Screener.in top ratios for price + 52W data.
Posts ALL stocks to website so you can verify prices are correct.
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

    print(f"\nFetching price + 52W data for {total} stocks from Screener.in...\n")

    all_stocks    = []
    opportunities = []
    errors        = []

    for i, symbol in enumerate(symbols, 1):
        print(f"[{i}/{total}] {symbol}", end=" ", flush=True)
        try:
            raw = fetch(symbol, delay=2.0)

            if "error" in raw:
                print(f"-- ERROR: {raw['error']}")
                errors.append(symbol)
                continue

            pd   = raw.get("price_data", {})
            info = meta_map.get(symbol, {})
            tags = classify(pd)

            entry = {
                "symbol":             symbol,
                "name":               raw.get("name") or info.get("name", symbol),
                "sector":             raw.get("sector") or info.get("sector", ""),
                "current_price":      pd.get("current_price"),
                "week52_high":        pd.get("week52_high"),
                "week52_low":         pd.get("week52_low"),
                "pct_above_52w_low":  pd.get("pct_above_52w_low"),
                "pct_below_52w_high": pd.get("pct_below_52w_high"),
                "near_52w_low":       pd.get("near_52w_low", False),
                "change_1m_pct":      None,
                "change_3m_pct":      None,
                "tags":               tags,
                "is_opportunity":     len(tags) > 0,
                "screener_url":       raw.get("url", ""),
                "top_ratios":         raw.get("top_ratios", {}),
            }

            price_str = f"₹{pd.get('current_price')} | 52W: ₹{pd.get('week52_low')}-₹{pd.get('week52_high')}"
            print(f"-- {price_str} | tags={tags or 'none'}")

            all_stocks.append(entry)
            if tags:
                opportunities.append(entry)

        except Exception as e:
            print(f"-- EXCEPTION: {e}")
            errors.append(symbol)

    # Sort opportunities by closest to 52W low
    priority = {"AT_52W_LOW": 0, "NEAR_52W_LOW": 1, "DISCOUNTED_ZONE": 2, "NEAR_52W_HIGH": 3}
    opportunities.sort(key=lambda x: min((priority.get(t, 9) for t in x["tags"]), default=9))
    all_stocks.sort(key=lambda x: (x.get("pct_above_52w_low") or 999))

    priced = sum(1 for s in all_stocks if s["current_price"])
    payload = {
        "meta": {
            "strategy":      "opportunities",
            "scanned_at":    datetime.now(timezone.utc).isoformat(),
            "total":         total,
            "success":       priced,
            "opportunities": len(opportunities),
            "errors":        len(errors),
            "data_source":   "Screener.in top ratios",
        },
        "opportunities": opportunities,
        "all_stocks":    all_stocks,
    }

    for path in [ROOT / "results" / "opportunities.json", DOCS_DIR / "opportunities.json"]:
        with open(path, "w") as f:
            json.dump(payload, f, indent=2, default=str)

    near_low = [s for s in opportunities
                if any(t in s["tags"] for t in ["AT_52W_LOW", "NEAR_52W_LOW", "DISCOUNTED_ZONE"])]

    print(f"\n{'='*60}")
    print(f"  DONE: {priced}/{total} priced | {len(errors)} errors")
    print(f"  Opportunities: {len(opportunities)} | Near 52W low: {len(near_low)}")
    if near_low:
        print(f"\n  Top near 52W low:")
        for s in near_low[:10]:
            print(f"    {s['symbol']:12} ₹{s['current_price']} | "
                  f"52W low ₹{s['week52_low']} | +{s['pct_above_52w_low']}% above low")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    run()
