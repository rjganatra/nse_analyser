#!/usr/bin/env python3
"""
scan_fundamentals.py
Strategy 1: Weekly fundamental scan of all Nifty 500 stocks.
Fetches P&L, Balance Sheet, Cash Flow from Screener.in.
Scores all 13 criteria. Writes results to docs/ for GitHub Pages.
Runtime: ~30-45 minutes for 500 stocks (polite 2s delay per stock).
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from fetchers.universe import fetch_nifty500
from fetchers.screener import fetch
from analyzer.scorer import score

ROOT = Path(__file__).parent
RESULTS_DIR    = ROOT / "results" / "fundamentals"
DOCS_FUND_DIR  = ROOT / "docs" / "fundamentals"

RESULTS_DIR.mkdir(parents=True, exist_ok=True)
DOCS_FUND_DIR.mkdir(parents=True, exist_ok=True)


def run():
    print(f"\n{'='*60}")
    print(f"  STRATEGY 1: FUNDAMENTAL SCAN")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}")

    # Step 1: Get full Nifty 500 universe
    universe = fetch_nifty500()
    symbols  = [s["symbol"] for s in universe]
    name_map = {s["symbol"]: s["name"] for s in universe}
    sec_map  = {s["symbol"]: s["sector"] for s in universe}

    print(f"\nScanning {len(symbols)} stocks...\n")

    summary = []
    errors  = []
    total   = len(symbols)

    for i, symbol in enumerate(symbols, 1):
        print(f"[{i}/{total}] {symbol}", end=" ")
        try:
            raw = fetch(symbol, delay=2.0)

            # Fill name/sector from NSE universe if Screener didn't return them
            if not raw.get("name") or raw["name"] == symbol:
                raw["name"] = name_map.get(symbol, symbol)
            if not raw.get("sector"):
                raw["sector"] = sec_map.get(symbol, "")

            if "error" in raw:
                print(f"-- ERROR: {raw['error']}")
                errors.append({"symbol": symbol, "error": raw["error"]})
                continue

            result = score(raw)
            result["scanned_at"] = datetime.now(timezone.utc).isoformat()

            # Save individual detail file
            with open(RESULTS_DIR / f"{symbol}.json", "w") as f:
                json.dump(result, f, indent=2, default=str)

            # Copy to docs for GitHub Pages
            with open(DOCS_FUND_DIR / f"{symbol}.json", "w") as f:
                json.dump(result, f, default=str)

            summary.append({
                "symbol":       result["symbol"],
                "name":         result["name"],
                "sector":       result["sector"],
                "score_pct":    result["score_pct"],
                "passes":       result["passes"],
                "fails":        result["fails"],
                "neutrals":     result["neutrals"],
                "verdict":      result["verdict"],
                "verdict_color":result["verdict_color"],
                "screener_url": result["screener_url"],
                "top_ratios":   result.get("top_ratios", {}),
                "scanned_at":   result["scanned_at"],
            })
            print(f"-- {result['score_pct']}% ({result['verdict']})")

        except Exception as e:
            print(f"-- EXCEPTION: {e}")
            errors.append({"symbol": symbol, "error": str(e)})

    # Sort by score
    summary.sort(key=lambda x: x["score_pct"], reverse=True)

    payload = {
        "meta": {
            "strategy":   "fundamentals",
            "scanned_at": datetime.now(timezone.utc).isoformat(),
            "total":      total,
            "success":    len(summary),
            "errors":     len(errors),
            "error_list": [e["symbol"] for e in errors],
        },
        "stocks": summary,
    }

    # Write summary files
    for path in [ROOT / "results" / "fundamentals_summary.json",
                 ROOT / "docs" / "fundamentals_summary.json"]:
        with open(path, "w") as f:
            json.dump(payload, f, indent=2, default=str)

    print(f"\n{'='*60}")
    print(f"  DONE: {len(summary)}/{total} scanned | {len(errors)} errors")
    print(f"  Top 5:")
    for s in summary[:5]:
        print(f"    {s['symbol']:15} {s['score_pct']}%  {s['verdict']}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    run()
