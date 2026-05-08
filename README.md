# FundaScreen — NSE/BSE Stock Analyzer

Automated stock screener for Nifty 500 stocks. Two independent strategies run on GitHub Actions and publish results to a GitHub Pages website. No server needed, no paid APIs, fully automated.

---

## What it does

### Strategy 1 — Fundamental Scan (Weekly, Sundays 7:30 AM IST)
Scores every Nifty 500 stock against 13 fundamental criteria:

| # | Criterion | Weight |
|---|-----------|--------|
| 1 | Sales growth >= industry average | 1x |
| 2 | Operating profit margin > industry | 1x |
| 3 | EPS consistently increasing | 1.5x |
| 4 | Net profit margin >= industry | 1x |
| 5 | Reserves and surplus increasing | 1x |
| 6 | Low / no debt (D/E < 0.3) | 1.5x |
| 7 | Cash on balance sheet increasing | 1x |
| 8 | Fixed assets increasing (capex) | 0.8x |
| 9 | Negative NWC (bargaining power) | 1x |
| 10 | CFO positive and increasing | 2x |
| 11 | CFI negative (investing in growth) | 1x |
| 12 | CFF: debt repayment or growth | 1x |
| 13 | ROE > ROCE | 1.5x |

### Strategy 2 — Opportunity Scan (Daily weekdays, 5:00 PM IST)
Scans all 500 stocks for price-based opportunities:
- Stocks at or near 52-week low (within 30%) — discounted entry points
- Stocks near 52-week high — momentum plays
- Full ratios: P/E, Industry P/E, P/B, ROE, ROCE, D/E, Dividend Yield

---

## Project structure

```
├── scan_fundamentals.py         ← Strategy 1 entry point
├── scan_opportunities.py        ← Strategy 2 entry point
├── fetchers/
│   ├── universe.py              ← Fetches Nifty 500 list from NSE (auto-updates)
│   ├── screener.py              ← Scrapes Screener.in (price, 52W, ratios, tables)
│   └── fundamentals.py          ← yfinance fallback for fundamental data
├── analyzer/
│   └── scorer.py                ← 13-criteria weighted scoring engine
├── docs/
│   └── index.html               ← GitHub Pages website
├── results/                     ← Scan outputs (committed by Actions)
├── custom_watchlist.json        ← User-added stocks (synced from website)
├── .github/workflows/
│   ├── opportunity-scan.yml     ← Daily 5PM IST
│   └── fundamental-scan.yml     ← Weekly Sunday 7:30AM IST
```

---

## Setup

### 1. Fork / create repo

```bash
git clone https://github.com/rjganatra/stock-analyzer.git
cd stock-analyzer
```

### 2. Enable GitHub Pages

Go to your repo → **Settings → Pages → Source → GitHub Actions** → Save.

This is required for the website to work. Do this once.

### 3. Run the first scan

Go to **Actions tab → Opportunity Scan (Daily) → Run workflow**.

This fetches live data and deploys the website. Takes about 20-30 minutes for 500 stocks.

Your site will be live at: `https://rjganatra.github.io/stock-analyzer`

### 4. Connect watchlist sync (optional)

Open the website → click the ⚙ gear icon → paste a GitHub Personal Access Token (repo scope).

Get a token at: https://github.com/settings/tokens

Once set, starring stocks on the website automatically syncs to `custom_watchlist.json` in the repo.

---

## Automatic schedule

| Workflow | Schedule | What it does |
|----------|----------|--------------|
| Opportunity Scan | Weekdays 5:00 PM IST | Price + 52W data for all 500 stocks |
| Fundamental Scan | Sundays 7:30 AM IST | Full 13-criteria scoring |

Both workflows automatically deploy the website after finishing. You never need to manually trigger anything after the first run.

> **Note:** GitHub pauses scheduled workflows on repos that have been inactive for 60 days. Just visit your repo or make any small commit to reactivate.

---

## Website features

- **Opportunities tab** — All stocks sorted by proximity to 52W low. Visual price bar shows where current price sits between 52W low and high.
- **Fundamentals tab** — All 500 stocks scored and ranked. Click any stock for full 13-criteria breakdown.
- **Watchlist** — Star stocks to save them. Syncs to repo if GitHub token is configured.
- **Filters** — Filter by sector, verdict (Strong/Moderate/Weak), price signal (Near 52W Low etc.)
- **Sort** — Sort by price, P/E, ROE, score, proximity to 52W low
- **Light/Dark mode** — Toggle with the ☀/☾ button
- **Responsive** — Works on mobile and desktop

---

## Data sources

| Data | Source | Reliability |
|------|--------|-------------|
| Nifty 500 ticker list | NSE official CSV | Auto-updates on index reconstitution |
| Current price, 52W High/Low | Screener.in top ratios | Works from GitHub Actions |
| P/E, P/B, ROE, ROCE, D/E | Screener.in top ratios | Works from GitHub Actions |
| P&L, Balance Sheet, Cash Flow | Screener.in tables | May be blocked on AWS IPs |

---

## Verdicts

| Score | Verdict |
|-------|---------|
| >= 78% | Strong buy candidate |
| 58–78% | Moderate — dig deeper |
| 40–58% | Weak fundamentals |
| < 40% | Avoid |

---

## Adding stocks to watchlist manually

Edit `custom_watchlist.json` in the repo root:

```json
{
  "custom_stocks": ["TITAN", "HDFCBANK", "INFY"],
  "notes": "Stocks added here are included in every scan"
}
```

Commit the change and the next scan will include these stocks.

---

*Built by Raj Ganatra · Data from NSE and Screener.in*
