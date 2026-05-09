"""
fetchers/screener.py
Scrapes Screener.in top ratios. Works from GitHub Actions IPs.
Industry P/E is parsed from the full page text as fallback since
Screener sometimes renders it outside the standard li structure.
"""

import re
import time
import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def _get(symbol: str) -> tuple[BeautifulSoup | None, str]:
    for path in ["/consolidated/", "/"]:
        url = f"https://www.screener.in/company/{symbol}{path}"
        try:
            r = requests.get(url, headers=HEADERS, timeout=25)
            if r.status_code == 200:
                soup = BeautifulSoup(r.text, "lxml")
                h1 = soup.find("h1")
                if h1 and "not found" in h1.get_text().lower():
                    return None, ""
                if soup.find(id="top-ratios") or soup.find("section"):
                    return soup, url
        except Exception as e:
            print(f"    [screener] {symbol} error: {e}")
    return None, ""


def _num(text: str) -> float | None:
    if not text:
        return None
    c = re.sub(r"[,%₹\s]", "", str(text).strip())
    c = re.sub(r"[^\d.\-]", "", c)
    try:
        return float(c) if c else None
    except ValueError:
        return None


def _parse_top_ratios(soup: BeautifulSoup) -> dict:
    out = {}
    section = soup.find(id="top-ratios") or soup

    for li in section.select("li"):
        name_el = li.find("span", class_="name")
        val_el  = li.find("span", class_="value")
        if not name_el or not val_el:
            continue
        name = name_el.get_text(strip=True)
        raw  = val_el.get_text(strip=True)

        # 52W High / Low field
        if "High" in name and "Low" in name:
            parts = re.split(r"\s*/\s*", raw)
            if len(parts) == 2:
                out["week52_high"] = _num(parts[0])
                out["week52_low"]  = _num(parts[1])
            continue

        val = _num(raw)
        if val is not None:
            out[name] = val

    # Normalise common key names
    key_map = {
        "Current Price":       "current_price",
        "Stock P/E":           "pe",
        "P/E":                 "pe",
        "Industry P/E":        "industry_pe",
        "P/B":                 "pb",
        "Price to Book Value": "pb",
        "Market Cap":          "market_cap",
        "Book Value":          "book_value",
        "Dividend Yield":      "div_yield",
        "ROCE":                "roce",
        "ROE":                 "roe",
        "Debt / Equity":       "debt_to_equity",
        "Face Value":          "face_value",
    }
    mapped = {}
    for k, v in out.items():
        mapped[key_map.get(k, k)] = v

    # ── Industry P/E fallback: scan full page text ────────────────────────
    # Screener sometimes renders it as plain text outside the li structure
    if "industry_pe" not in mapped or mapped["industry_pe"] is None:
        page_text = soup.get_text(" ", strip=True)
        # Patterns: "Industry P/E 45.2" or "Industry PE: 45.2"
        m = re.search(
            r"[Ii]ndustry\s+P[/\s]?E[:\s]+([0-9]+\.?[0-9]*)",
            page_text
        )
        if m:
            mapped["industry_pe"] = float(m.group(1))

    mapped["_raw"] = out
    return mapped


def _parse_table(soup: BeautifulSoup, *section_ids) -> dict:
    for sid in section_ids:
        sec = soup.find("section", {"id": sid})
        if not sec:
            continue
        table = sec.find("table")
        if not table:
            continue
        out = {}
        for row in table.find_all("tr"):
            cells = row.find_all(["th", "td"])
            if len(cells) < 2:
                continue
            label = cells[0].get_text(strip=True)
            vals  = [_num(c.get_text(strip=True)) for c in cells[1:]]
            if label and any(v is not None for v in vals):
                out[label] = vals
        if out:
            return out
    return {}


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


def _get_row(table: dict, *keys) -> list:
    for k in keys:
        if k in table:
            return table[k]
        for tk in table:
            if k.lower() in tk.lower():
                return table[tk]
    return []


def fetch(symbol: str, delay: float = 2.0, nse_sector: str = "") -> dict:
    symbol = symbol.upper().strip()
    time.sleep(delay)

    soup, url = _get(symbol)
    if soup is None:
        return {"error": f"{symbol} not found on Screener.in"}

    h1   = soup.find("h1")
    name = h1.get_text(strip=True) if h1 else symbol

    ratios        = _parse_top_ratios(soup)
    current_price = ratios.get("current_price")
    week52_high   = ratios.get("week52_high")
    week52_low    = ratios.get("week52_low")
    roe_val       = ratios.get("roe")
    roce_val      = ratios.get("roce")
    d2e           = ratios.get("debt_to_equity")
    pe_val        = ratios.get("pe")
    industry_pe   = ratios.get("industry_pe")
    pb_val        = ratios.get("pb")
    div_yield     = ratios.get("div_yield")
    market_cap    = ratios.get("market_cap")

    pct_above_52w_low  = None
    pct_below_52w_high = None
    near_52w_low       = False
    if current_price and week52_low and week52_low > 0:
        pct_above_52w_low = round(((current_price - week52_low) / week52_low) * 100, 1)
        near_52w_low      = pct_above_52w_low <= 30
    if current_price and week52_high and week52_high > 0:
        pct_below_52w_high = round(((week52_high - current_price) / week52_high) * 100, 1)

    pl  = _parse_table(soup, "profit-loss")
    bs  = _parse_table(soup, "balance-sheet")
    cf  = _parse_table(soup, "cash-flow")
    rat = _parse_table(soup, "ratios")

    sales  = _get_row(pl,  "Sales", "Revenue", "Net Sales")
    opm    = _get_row(pl,  "OPM %") or _get_row(rat, "OPM %")
    npm    = _get_row(pl,  "NPM %", "Net Profit Margin")
    eps    = _get_row(pl,  "EPS in Rs") or _get_row(rat, "EPS in Rs")
    res    = _get_row(bs,  "Reserves", "Reserves and Surplus")
    debt   = _get_row(bs,  "Borrowings", "Total Debt")
    fa     = _get_row(bs,  "Fixed Assets", "Net Block")
    cash   = _get_row(bs,  "Cash Equivalents", "Cash & Bank", "Cash")
    pay    = _get_row(bs,  "Trade Payables", "Creditors")
    rec    = _get_row(bs,  "Trade Receivables", "Debtors")
    inv    = _get_row(bs,  "Inventories", "Inventory")
    cfo    = _get_row(cf,  "Cash from Operating Activity", "Operating Activity")
    cfi    = _get_row(cf,  "Cash from Investing Activity", "Investing Activity")
    cff    = _get_row(cf,  "Cash from Financing Activity", "Financing Activity")
    roe_s  = _get_row(rat, "Return on Equity %", "ROE %")
    roce_s = _get_row(rat, "ROCE %", "Return on Capital Employed")

    if not roe_s  and roe_val:  roe_s  = [roe_val]
    if not roce_s and roce_val: roce_s = [roce_val]

    nwc = ((_last(rec) or 0) + (_last(inv) or 0)) - (_last(pay) or 0)
    tables_found = [n for n, t in [("P&L", pl), ("BS", bs), ("CF", cf), ("Ratios", rat)] if t]

    print(f"    tables={tables_found or 'none'} | "
          f"price=₹{current_price} | 52W={week52_low}-{week52_high} | "
          f"PE={pe_val} IndPE={industry_pe} ROE={roe_val} ROCE={roce_val} D/E={d2e}")

    return {
        "symbol": symbol, "name": name,
        "sector": nse_sector,  # always from NSE universe
        "url": url,
        "top_ratios": {
            "Current Price":  current_price,
            "Market Cap":     market_cap,
            "P/E":            pe_val,
            "Industry P/E":   industry_pe,
            "P/B":            pb_val,
            "ROE %":          roe_val,
            "ROCE %":         roce_val,
            "Debt / Equity":  d2e,
            "Div. Yield %":   div_yield,
            "52W High":       week52_high,
            "52W Low":        week52_low,
        },
        "price_data": {
            "current_price":      current_price,
            "week52_high":        week52_high,
            "week52_low":         week52_low,
            "pct_above_52w_low":  pct_above_52w_low,
            "pct_below_52w_high": pct_below_52w_high,
            "near_52w_low":       near_52w_low,
        },
        "series": {
            "sales": sales, "opm_pct": opm, "npm_pct": npm, "eps": eps,
            "reserves": res, "debt": debt, "fixed_assets": fa, "cash_bs": cash,
            "cfo": cfo, "cfi": cfi, "cff": cff, "roe": roe_s, "roce": roce_s,
        },
        "derived": {
            "sales_avg_growth":        _growth(sales),
            "eps_increasing":          _increasing(eps),
            "reserves_increasing":     _increasing(res),
            "cash_increasing":         _increasing(cash),
            "fixed_assets_increasing": _increasing(fa),
            "cfo_positive":            (_last(cfo) or 0) > 0,
            "cfo_increasing":          _increasing(cfo),
            "cfi_negative":            (_last(cfi) or 0) < 0,
            "nwc":                     round(nwc, 2),
            "nwc_negative":            nwc < 0,
            "debt_to_equity":          d2e,
            "roe":                     _last(roe_s),
            "roce":                    _last(roce_s),
            "opm":                     _last(opm),
            "npm":                     _last(npm),
            "last_cfo":                _last(cfo),
            "last_cfi":                _last(cfi),
            "last_cff":                _last(cff),
        },
    }
