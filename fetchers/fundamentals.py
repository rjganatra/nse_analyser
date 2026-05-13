"""
fetchers/fundamentals.py
Fetches fundamental data via yfinance — single threaded, 5s delay.
Slow but reliable. 500 stocks ~ 42 minutes. No rate limiting.
"""

import time
import json
from pathlib import Path

import yfinance as yf
import pandas as pd

ROOT = Path(__file__).parent.parent

SPECIAL = {
    "M&M": "M%26M.NS", "M&MFIN": "M%26MFIN.NS",
    "J&KBANK": "J%26KBANK.NS", "ARE&M": "ARE%26M.NS", "GVT&D": "GVT%26D.NS"
}


def _nse(symbol: str) -> str:
    return SPECIAL.get(symbol, f"{symbol}.NS")


def _safe(val) -> float | None:
    try:
        if val is None:
            return None
        f = float(val)
        return None if pd.isna(f) else f
    except Exception:
        return None


def _row(df: pd.DataFrame, *keys) -> list:
    if df is None or df.empty:
        return []
    for k in keys:
        if k in df.index:
            return [_safe(v) for v in reversed(df.loc[k].values)]
        for idx in df.index:
            if k.lower() in str(idx).lower():
                return [_safe(v) for v in reversed(df.loc[idx].values)]
    return []


def _last(lst) -> float | None:
    c = [v for v in (lst or []) if v is not None]
    return c[-1] if c else None


def _increasing(lst, n=3) -> bool | None:
    c = [v for v in (lst or []) if v is not None]
    if len(c) < n:
        return None
    return c[-1] > c[0]


def _growth(lst) -> float | None:
    c = [v for v in (lst or []) if v is not None]
    if len(c) < 2:
        return None
    g = [(c[i]-c[i-1])/abs(c[i-1])*100
         for i in range(1, len(c)) if c[i-1] and c[i-1] != 0]
    return round(sum(g)/len(g), 2) if g else None


def fetch(symbol: str, delay: float = 5.0) -> dict:
    symbol = symbol.upper().strip()
    time.sleep(delay)

    try:
        obj  = yf.Ticker(_nse(symbol))
        info = obj.info or {}

        fin  = obj.financials
        bal  = obj.balance_sheet
        cf   = obj.cashflow

        if not info and (fin is None or fin.empty):
            return {"error": f"{symbol}: no data"}

        sales    = _row(fin, "Total Revenue", "Revenue")
        op_inc   = _row(fin, "Operating Income", "EBIT")
        net_p    = _row(fin, "Net Income")
        reserves = _row(bal, "Retained Earnings", "Stockholders Equity")
        total_eq = _row(bal, "Stockholders Equity", "Common Stock Equity")
        debt_s   = _row(bal, "Total Debt", "Long Term Debt")
        fixed_a  = _row(bal, "Net PPE", "Property Plant Equipment Net")
        cash_s   = _row(bal, "Cash And Cash Equivalents",
                        "Cash Cash Equivalents And Short Term Investments")
        rec_s    = _row(bal, "Receivables", "Accounts Receivable")
        inv_s    = _row(bal, "Inventory")
        pay_s    = _row(bal, "Payables", "Accounts Payable")
        cfo      = _row(cf, "Operating Cash Flow",
                        "Cash Flow From Continuing Operating Activities")
        cfi      = _row(cf, "Investing Cash Flow",
                        "Cash Flow From Continuing Investing Activities")
        cff      = _row(cf, "Financing Cash Flow",
                        "Cash Flow From Continuing Financing Activities")

        opm, npm = [], []
        for s, o in zip(sales, op_inc):
            opm.append(round(o/s*100, 1) if s and s != 0 and o is not None else None)
        for s, n in zip(sales, net_p):
            npm.append(round(n/s*100, 1) if s and s != 0 and n is not None else None)

        roe_val = _safe(info.get("returnOnEquity"))
        roe_val = round(roe_val * 100, 1) if roe_val else None

        roce_val = None
        ebit_v   = _last(op_inc)
        t_assets = _safe(info.get("totalAssets"))
        c_liab   = _safe(info.get("totalCurrentLiabilities") or info.get("currentLiabilities"))
        if ebit_v and t_assets and c_liab:
            cap_emp  = t_assets - c_liab
            roce_val = round(ebit_v / cap_emp * 100, 1) if cap_emp > 0 else None

        d2e = _safe(info.get("debtToEquity"))
        if d2e:
            d2e = round(d2e / 100, 2)

        nwc = ((_last(rec_s) or 0) + (_last(inv_s) or 0)) - (_last(pay_s) or 0)

        eps_val = _safe(info.get("trailingEps") or info.get("epsTrailingTwelveMonths"))

        return {
            "symbol": symbol,
            "name":   info.get("longName") or info.get("shortName") or symbol,
            "sector": info.get("sector") or info.get("industry") or "",
            "url":    f"https://www.screener.in/company/{symbol}/consolidated/",
            "top_ratios": {
                "Market Cap":    _safe(info.get("marketCap")),
                "P/E":           _safe(info.get("trailingPE")),
                "ROE %":         roe_val,
                "ROCE %":        roce_val,
                "Debt / Equity": d2e,
                "Div. Yield %":  round((_safe(info.get("dividendYield")) or 0) * 100, 2),
                "Current Price": _safe(info.get("currentPrice") or info.get("regularMarketPrice")),
            },
            "series": {
                "sales": sales, "opm_pct": opm, "npm_pct": npm,
                "eps": [eps_val] if eps_val else [],
                "reserves": reserves, "debt": debt_s, "fixed_assets": fixed_a,
                "cash_bs": cash_s, "cfo": cfo, "cfi": cfi, "cff": cff,
                "roe": [roe_val] if roe_val else [],
                "roce": [roce_val] if roce_val else [],
            },
            "derived": {
                "sales_avg_growth":        _growth(sales),
                "eps_increasing":          None,
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
