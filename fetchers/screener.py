"""
fetchers/screener.py
Scrapes fundamental data from Screener.in public pages.
"""

import re
import time
import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}


def _get(url: str) -> BeautifulSoup | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code == 404:
            r = requests.get(url.replace("/consolidated/", "/"), headers=HEADERS, timeout=20)
        if r.status_code != 200:
            return None
        return BeautifulSoup(r.text, "lxml")
    except Exception as e:
        print(f"    [screener] error: {e}")
        return None


def _num(text: str) -> float | None:
    if not text:
        return None
    c = re.sub(r"[,%\s]", "", text.strip())
    c = re.sub(r"[^\d.\-]", "", c)
    try:
        return float(c)
    except ValueError:
        return None


def _table(soup, sid: str) -> dict:
    sec = soup.find("section", {"id": sid})
    if not sec:
        return {}
    tbl = sec.find("table")
    if not tbl:
        return {}
    out = {}
    for row in tbl.find_all("tr"):
        cells = row.find_all(["th", "td"])
        if len(cells) < 2:
            continue
        out[cells[0].get_text(strip=True)] = [_num(c.get_text(strip=True)) for c in cells[1:]]
    return out


def _ratios_bar(soup) -> dict:
    out = {}
    for li in soup.select("#top-ratios li"):
        n = li.find("span", class_="name")
        v = li.find("span", class_="value")
        if n and v:
            out[n.get_text(strip=True)] = _num(v.get_text(strip=True))
    return out


def _last(lst) -> float | None:
    c = [v for v in lst if v is not None]
    return c[-1] if c else None


def _increasing(lst, n=3) -> bool | None:
    c = [v for v in lst if v is not None]
    if len(c) < n:
        return None
    return c[-1] > c[0]


def _avg_growth(lst) -> float | None:
    c = [v for v in lst if v is not None]
    if len(c) < 2:
        return None
    g = [(c[i] - c[i-1]) / abs(c[i-1]) * 100 for i in range(1, len(c)) if c[i-1] != 0]
    return round(sum(g) / len(g), 2) if g else None


def fetch(symbol: str, delay: float = 2.0) -> dict:
    symbol = symbol.upper().strip()
    time.sleep(delay)
    url = f"https://www.screener.in/company/{symbol}/consolidated/"
    soup = _get(url)

    if soup is None:
        return {"error": f"Could not reach Screener.in for {symbol}"}
    h1 = soup.find("h1")
    if h1 and "not found" in h1.get_text().lower():
        return {"error": f"{symbol} not found on Screener.in"}

    pl     = _table(soup, "profit-loss")
    bs     = _table(soup, "balance-sheet")
    cf     = _table(soup, "cash-flow")
    ratios = _table(soup, "ratios")
    top    = _ratios_bar(soup)

    name = h1.get_text(strip=True) if h1 else symbol
    sec_el = soup.select_one(".company-links a")
    sector = sec_el.get_text(strip=True) if sec_el else ""

    sales = pl.get("Sales", pl.get("Revenue", []))
    opm   = pl.get("OPM %", ratios.get("OPM %", []))
    npm   = pl.get("NPM %", [])
    eps   = pl.get("EPS in Rs", ratios.get("EPS in Rs", []))
    res   = bs.get("Reserves", [])
    debt  = bs.get("Borrowings", bs.get("Total Debt", []))
    fa    = bs.get("Fixed Assets", bs.get("Net Block", []))
    cash  = bs.get("Cash Equivalents", bs.get("Cash & Bank", []))
    pay   = bs.get("Trade Payables", [])
    rec   = bs.get("Trade Receivables", [])
    inv   = bs.get("Inventories", [])
    cfo   = cf.get("Cash from Operating Activity", cf.get("Operating Activity", []))
    cfi   = cf.get("Cash from Investing Activity", cf.get("Investing Activity", []))
    cff   = cf.get("Cash from Financing Activity", cf.get("Financing Activity", []))
    roe_s = ratios.get("Return on Equity %", ratios.get("ROE %", []))
    roce_s = ratios.get("ROCE %", [])

    nwc = ((_last(rec) or 0) + (_last(inv) or 0)) - (_last(pay) or 0)
    d2e = top.get("Debt / Equity")
    if d2e is None:
        dv = _last(debt); ev = _last(bs.get("Equity Capital", []))
        d2e = round(dv / ev, 2) if dv and ev and ev != 0 else None

    return {
        "symbol": symbol, "name": name, "sector": sector, "url": url,
        "top_ratios": top,
        "series": {
            "sales": sales, "opm_pct": opm, "npm_pct": npm, "eps": eps,
            "reserves": res, "debt": debt, "fixed_assets": fa, "cash_bs": cash,
            "cfo": cfo, "cfi": cfi, "cff": cff, "roe": roe_s, "roce": roce_s,
        },
        "derived": {
            "sales_avg_growth": _avg_growth(sales),
            "eps_increasing": _increasing(eps),
            "reserves_increasing": _increasing(res),
            "cash_increasing": _increasing(cash),
            "fixed_assets_increasing": _increasing(fa),
            "cfo_positive": (_last(cfo) or 0) > 0,
            "cfo_increasing": _increasing(cfo),
            "cfi_negative": (_last(cfi) or 0) < 0,
            "nwc": round(nwc, 2),
            "nwc_negative": nwc < 0,
            "debt_to_equity": d2e,
            "roe": _last(roe_s),
            "roce": _last(roce_s),
            "opm": _last(opm),
            "npm": _last(npm),
            "last_cfo": _last(cfo),
            "last_cfi": _last(cfi),
            "last_cff": _last(cff),
        },
    }
