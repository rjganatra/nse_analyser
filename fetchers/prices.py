"""
fetchers/screener.py
Scrapes Screener.in fundamental data.
Robust parsing — tries multiple section IDs and fallback strategies.
"""

import re
import time
import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

# Try consolidated first, then standalone
URLS = ["https://www.screener.in/company/{symbol}/consolidated/",
        "https://www.screener.in/company/{symbol}/"]

# Screener.in section IDs — try all variants in case they change
PL_IDS  = ["profit-loss", "profit_loss", "income-statement"]
BS_IDS  = ["balance-sheet", "balance_sheet"]
CF_IDS  = ["cash-flow", "cash_flow", "cashflow"]
RAT_IDS = ["ratios", "key-ratios", "financial-ratios"]


def _get(symbol: str) -> tuple[BeautifulSoup | None, str]:
    session = requests.Session()
    for url_tpl in URLS:
        url = url_tpl.format(symbol=symbol)
        try:
            r = session.get(url, headers=HEADERS, timeout=25)
            if r.status_code == 200:
                soup = BeautifulSoup(r.text, "lxml")
                # Check it's a real company page not a 404/redirect
                if soup.find("section", {"id": "profit-loss"}) or \
                   soup.find("section", {"id": "balance-sheet"}) or \
                   soup.find("table"):
                    return soup, url
        except Exception as e:
            print(f"    [screener] {url} error: {e}")
    return None, ""


def _num(text: str) -> float | None:
    if not text:
        return None
    c = re.sub(r"[,%\s]", "", str(text).strip())
    c = re.sub(r"[^\d.\-]", "", c)
    try:
        v = float(c)
        return None if (v == 0 and c == "") else v
    except ValueError:
        return None


def _find_section(soup, ids: list[str]) -> BeautifulSoup | None:
    for sid in ids:
        sec = soup.find("section", {"id": sid})
        if sec:
            return sec
        # Also try data-id attribute
        sec = soup.find("section", attrs={"data-id": sid})
        if sec:
            return sec
    return None


def _parse_table(section) -> dict:
    """Parse a Screener.in data table into {label: [val1, val2, ...]}"""
    if not section:
        return {}
    table = section.find("table")
    if not table:
        return {}
    out = {}
    for row in table.find_all("tr"):
        cells = row.find_all(["th", "td"])
        if len(cells) < 2:
            continue
        label = cells[0].get_text(strip=True)
        vals  = [_num(c.get_text(strip=True)) for c in cells[1:]]
        if label and any(v is not None for v in vals):
            out[label] = vals
    return out


def _parse_ratios_bar(soup) -> dict:
    """Parse the top ratios bar (Market Cap, P/E, ROE, etc.)"""
    out = {}
    # Method 1: standard list items
    for li in soup.select("#top-ratios li"):
        n = li.find("span", class_="name")
        v = li.find("span", class_="value")
        if n and v:
            out[n.get_text(strip=True)] = _num(v.get_text(strip=True))

    # Method 2: any element with data-source="ratio"
    if not out:
        for el in soup.find_all(attrs={"data-source": "ratio"}):
            key = el.get("data-field", "") or el.get_text(strip=True)
            val = _num(el.get_text(strip=True))
            if key and val is not None:
                out[key] = val

    return out


def _last(lst) -> float | None:
    c = [v for v in (lst or []) if v is not None]
    return c[-1] if c else None


def _increasing(lst, n=3) -> bool | None:
    c = [v for v in (lst or []) if v is not None]
    if len(c) < n:
        return None
    return c[-1] > c[0]


def _avg_growth(lst) -> float | None:
    c = [v for v in (lst or []) if v is not None]
    if len(c) < 2:
        return None
    g = [(c[i]-c[i-1])/abs(c[i-1])*100 for i in range(1, len(c)) if c[i-1] != 0]
    return round(sum(g)/len(g), 2) if g else None


def _get_row(table: dict, *keys) -> list:
    """Try multiple key names for the same row."""
    for k in keys:
        if k in table:
            return table[k]
        # Partial match
        for tk in table:
            if k.lower() in tk.lower():
                return table[tk]
    return []


def fetch(symbol: str, delay: float = 2.0) -> dict:
    symbol = symbol.upper().strip()
    time.sleep(delay)

    soup, url = _get(symbol)
    if soup is None:
        return {"error": f"Could not reach Screener.in for {symbol}"}

    h1 = soup.find("h1")
    if h1 and "not found" in h1.get_text().lower():
        return {"error": f"{symbol} not found on Screener.in"}

    # Parse sections
    pl_sec  = _find_section(soup, PL_IDS)
    bs_sec  = _find_section(soup, BS_IDS)
    cf_sec  = _find_section(soup, CF_IDS)
    rat_sec = _find_section(soup, RAT_IDS)

    pl  = _parse_table(pl_sec)
    bs  = _parse_table(bs_sec)
    cf  = _parse_table(cf_sec)
    rat = _parse_table(rat_sec)
    top = _parse_ratios_bar(soup)

    # Debug: show what we found
    found = [s for s, t in [("P&L", pl), ("BS", bs), ("CF", cf), ("Ratios", rat)] if t]
    print(f"    [screener] {symbol}: found {found or 'NONE -- page may be blocked'}")

    name = h1.get_text(strip=True) if h1 else symbol
    sec_el = soup.select_one(".company-links a")
    sector = sec_el.get_text(strip=True) if sec_el else ""

    # Extract all series with multiple fallback key names
    sales  = _get_row(pl, "Sales", "Revenue", "Net Sales", "Total Revenue")
    opm    = _get_row(pl, "OPM %", "Operating Profit Margin %", "OPM") or _get_row(rat, "OPM %")
    npm    = _get_row(pl, "NPM %", "Net Profit Margin %", "NPM", "Net profit margin")
    eps    = _get_row(pl, "EPS in Rs", "EPS") or _get_row(rat, "EPS in Rs", "EPS")
    net_p  = _get_row(pl, "Net Profit", "Profit after tax", "PAT")
    res    = _get_row(bs, "Reserves", "Reserves and Surplus", "Reserves & Surplus")
    debt   = _get_row(bs, "Borrowings", "Total Debt", "Long Term Borrowing")
    fa     = _get_row(bs, "Fixed Assets", "Net Block", "Tangible Assets")
    cash   = _get_row(bs, "Cash Equivalents", "Cash & Bank", "Cash and Cash Equivalents", "Cash")
    pay    = _get_row(bs, "Trade Payables", "Creditors", "Accounts Payable")
    rec    = _get_row(bs, "Trade Receivables", "Debtors", "Accounts Receivable")
    inv    = _get_row(bs, "Inventories", "Inventory", "Stock in Trade")
    cfo    = _get_row(cf, "Cash from Operating Activity", "Operating Activity", "Cash from Operations", "Net Cash from Operating")
    cfi    = _get_row(cf, "Cash from Investing Activity", "Investing Activity", "Cash from Investing")
    cff    = _get_row(cf, "Cash from Financing Activity", "Financing Activity", "Cash from Financing")
    roe_s  = _get_row(rat, "Return on Equity %", "ROE %", "Return on equity") 
    roce_s = _get_row(rat, "ROCE %", "Return on Capital Employed %", "Return on capital employed")

    # NWC calculation
    nwc = ((_last(rec) or 0) + (_last(inv) or 0)) - (_last(pay) or 0)

    # D/E: try top ratios first, then compute
    d2e = top.get("Debt / Equity") or top.get("D/E")
    if d2e is None:
        dv = _last(debt)
        ev = _last(_get_row(bs, "Equity Capital", "Share Capital", "Paid up capital"))
        d2e = round(dv / ev, 2) if dv and ev and ev != 0 else None

    # ROE/ROCE: try top ratios if not in ratios table
    roe_val  = _last(roe_s)  or top.get("ROE %")  or top.get("Return on Equity %")
    roce_val = _last(roce_s) or top.get("ROCE %") or top.get("Return on Capital Employed %")

    return {
        "symbol": symbol, "name": name, "sector": sector, "url": url,
        "top_ratios": top,
        "debug": {"sections_found": found, "pl_rows": len(pl), "bs_rows": len(bs), "cf_rows": len(cf)},
        "series": {
            "sales": sales, "opm_pct": opm, "npm_pct": npm, "eps": eps,
            "reserves": res, "debt": debt, "fixed_assets": fa, "cash_bs": cash,
            "cfo": cfo, "cfi": cfi, "cff": cff, "roe": roe_s, "roce": roce_s,
        },
        "derived": {
            "sales_avg_growth":      _avg_growth(sales),
            "eps_increasing":        _increasing(eps),
            "reserves_increasing":   _increasing(res),
            "cash_increasing":       _increasing(cash),
            "fixed_assets_increasing": _increasing(fa),
            "cfo_positive":          (_last(cfo) or 0) > 0,
            "cfo_increasing":        _increasing(cfo),
            "cfi_negative":          (_last(cfi) or 0) < 0,
            "nwc":                   round(nwc, 2),
            "nwc_negative":          nwc < 0,
            "debt_to_equity":        d2e,
            "roe":                   roe_val,
            "roce":                  roce_val,
            "opm":                   _last(opm),
            "npm":                   _last(npm),
            "last_cfo":              _last(cfo),
            "last_cfi":              _last(cfi),
            "last_cff":              _last(cff),
        },
    }
