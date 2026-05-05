"""
fetchers/fundamentals.py
Fetches fundamental data using yfinance.
Works from any IP including GitHub Actions AWS servers.
Covers: P&L, Balance Sheet, Cash Flow, Key Ratios.
"""

import time
import json
import threading
from pathlib import Path
import yfinance as yf
import pandas as pd

ROOT = Path(__file__).parent.parent


def _nse(symbol: str) -> str:
    special = {"M&M": "M%26M.NS", "M&MFIN": "M%26MFIN.NS", "J&KBANK": "J%26KBANK.NS",
               "ARE&M": "ARE%26M.NS", "GVT&D": "GVT%26D.NS"}
    return special.get(symbol, f"{symbol}.NS")


def _safe(val):
    try:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        return float(val)
    except Exception:
        return None


def _growth(series: list) -> float | None:
    c = [v for v in series if v is not None]
    if len(c) < 2:
        return None
    g = [(c[i]-c[i-1])/abs(c[i-1])*100 for i in range(1, len(c)) if c[i-1] and c[i-1] != 0]
    return round(sum(g)/len(g), 2) if g else None


def _increasing(series: list, n: int = 3) -> bool | None:
    c = [v for v in series if v is not None]
    if len(c) < n:
        return None
    return c[-1] > c[0]


def _last(lst) -> float | None:
    c = [v for v in (lst or []) if v is not None]
    return c[-1] if c else None


def _row(df: pd.DataFrame, *keys) -> list:
    """Extract a row from a yfinance DataFrame trying multiple key names."""
    if df is None or df.empty:
        return []
    for k in keys:
        if k in df.index:
            vals = [_safe(v) for v in df.loc[k].values]
            return list(reversed(vals))  # yfinance is newest-first, reverse to oldest-first
        # Partial match
        matches = [i for i in df.index if k.lower() in str(i).lower()]
        if matches:
            vals = [_safe(v) for v in df.loc[matches[0]].values]
            return list(reversed(vals))
    return []


def fetch(symbol: str, delay: float = 1.0) -> dict:
    symbol = symbol.upper().strip()
    time.sleep(delay)
    ticker = _nse(symbol)

    try:
        obj = yf.Ticker(ticker)

        # Fetch all data
        info        = obj.info or {}
        financials  = obj.financials          # P&L (annual, newest first)
        balance     = obj.balance_sheet       # Balance sheet (annual)
        cashflow    = obj.cashflow            # Cash flow (annual)

        if info.get("quoteType") is None and (financials is None or financials.empty):
            return {"error": f"{symbol}: no data from yfinance"}

        # ── P&L ──────────────────────────────────────────────────────────────
        sales     = _row(financials, "Total Revenue", "Revenue")
        net_profit= _row(financials, "Net Income", "Net Income Common Stockholders")
        op_income = _row(financials, "Operating Income", "EBIT")
        gross_p   = _row(financials, "Gross Profit")
        ebit      = _row(financials, "EBIT", "Operating Income")
        interest  = _row(financials, "Interest Expense")

        # OPM = Operating Income / Revenue
        opm = []
        for s, o in zip(sales, op_income):
            if s and s != 0 and o is not None:
                opm.append(round(o/s*100, 1))
            else:
                opm.append(None)

        # NPM = Net Income / Revenue
        npm = []
        for s, n in zip(sales, net_profit):
            if s and s != 0 and n is not None:
                npm.append(round(n/s*100, 1))
            else:
                npm.append(None)

        # EPS from info
        eps_val = _safe(info.get("trailingEps") or info.get("epsTrailingTwelveMonths"))
        eps     = [eps_val] if eps_val else []

        # ── Balance Sheet ─────────────────────────────────────────────────────
        reserves = _row(balance, "Retained Earnings", "Stockholders Equity", "Total Equity Gross Minority Interest")
        total_eq = _row(balance, "Stockholders Equity", "Total Equity Gross Minority Interest", "Common Stock Equity")
        total_debt_s = _row(balance, "Total Debt", "Long Term Debt", "Total Liabilities Net Minority Interest")
        fixed_a  = _row(balance, "Net PPE", "Property Plant Equipment Net", "Fixed Assets")
        cash_s   = _row(balance, "Cash And Cash Equivalents", "Cash Cash Equivalents And Short Term Investments", "Cash")
        rec_s    = _row(balance, "Receivables", "Accounts Receivable", "Net Receivables")
        inv_s    = _row(balance, "Inventory", "Inventories")
        pay_s    = _row(balance, "Payables", "Accounts Payable", "Payables And Accrued Expenses")

        # ── Cash Flow ─────────────────────────────────────────────────────────
        cfo = _row(cashflow, "Operating Cash Flow", "Cash Flow From Continuing Operating Activities")
        cfi = _row(cashflow, "Investing Cash Flow", "Cash Flow From Continuing Investing Activities")
        cff = _row(cashflow, "Financing Cash Flow", "Cash Flow From Continuing Financing Activities")

        # ── Key Ratios from info ──────────────────────────────────────────────
        roe_val  = _safe(info.get("returnOnEquity"))
        roe_val  = round(roe_val * 100, 1) if roe_val else None
        roce_val = None  # yfinance doesn't provide ROCE directly, compute below

        # ROCE = EBIT / (Total Assets - Current Liabilities)
        ebit_last = _last(ebit)
        total_assets = _safe(info.get("totalAssets"))
        curr_liab    = _safe(info.get("totalCurrentLiabilities") or info.get("currentLiabilities"))
        if ebit_last and total_assets and curr_liab:
            cap_employed = total_assets - curr_liab
            if cap_employed > 0:
                roce_val = round(ebit_last / cap_employed * 100, 1)

        d2e  = _safe(info.get("debtToEquity"))
        if d2e:
            d2e = round(d2e / 100, 2)  # yfinance gives % form

        # NWC
        nwc = ((_last(rec_s) or 0) + (_last(inv_s) or 0)) - (_last(pay_s) or 0)

        top_ratios = {
            "Market Cap":    _safe(info.get("marketCap")),
            "P/E":           _safe(info.get("trailingPE") or info.get("forwardPE")),
            "ROE %":         roe_val,
            "ROCE %":        roce_val,
            "Debt / Equity": d2e,
            "Div. Yield %":  round(_safe(info.get("dividendYield") or 0) * 100, 2),
            "Current Price": _safe(info.get("currentPrice") or info.get("regularMarketPrice")),
        }

        screener_url = f"https://www.screener.in/company/{symbol}/consolidated/"
        sector = info.get("sector") or info.get("industry") or ""
        name   = info.get("longName") or info.get("shortName") or symbol

        return {
            "symbol": symbol, "name": name, "sector": sector,
            "url": screener_url, "top_ratios": top_ratios,
            "series": {
                "sales": sales, "opm_pct": opm, "npm_pct": npm, "eps": eps,
                "reserves": reserves, "debt": total_debt_s, "fixed_assets": fixed_a,
                "cash_bs": cash_s, "cfo": cfo, "cfi": cfi, "cff": cff,
                "roe": [roe_val] if roe_val else [],
                "roce": [roce_val] if roce_val else [],
            },
            "derived": {
                "sales_avg_growth":        _growth(sales),
                "eps_increasing":          _increasing(eps),
                "reserves_increasing":     _increasing(reserves),
                "cash_increasing":         _increasing(cash_s),
                "fixed_assets_increasing": _increasing(fixed_a),
                "cfo_positive":            (_last(cfo) or 0) > 0,
                "cfo_increasing":          _increasing(cfo),
                "cfi_negative":            (_last(cfi) or 0) < 0,
                "nwc":                     round(nwc, 2),
                "nwc_negative":            nwc < 0,
                "debt_to_equity":          d2e,
                "roe":                     roe_val,
                "roce":                    roce_val,
                "opm":                     _last(opm),
                "npm":                     _last(npm),
                "last_cfo":                _last(cfo),
                "last_cfi":                _last(cfi),
                "last_cff":                _last(cff),
            },
        }

    except Exception as e:
        return {"error": f"{symbol}: {str(e)}"}
