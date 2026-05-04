"""
analyzer/scorer.py — Scores 13 fundamental criteria. Weighted scoring.
"""

from __future__ import annotations

BENCHMARKS = {
    "default":     {"opm": 15.0, "npm": 8.0,  "sg": 10.0},
    "IT":          {"opm": 22.0, "npm": 18.0, "sg": 12.0},
    "Banking":     {"opm": 35.0, "npm": 20.0, "sg": 12.0},
    "NBFC":        {"opm": 30.0, "npm": 15.0, "sg": 14.0},
    "Pharma":      {"opm": 20.0, "npm": 12.0, "sg": 10.0},
    "FMCG":        {"opm": 18.0, "npm": 12.0, "sg": 8.0},
    "Auto":        {"opm": 12.0, "npm": 6.0,  "sg": 10.0},
    "Metal":       {"opm": 14.0, "npm": 7.0,  "sg": 8.0},
    "Cement":      {"opm": 18.0, "npm": 8.0,  "sg": 9.0},
    "Chemical":    {"opm": 18.0, "npm": 10.0, "sg": 11.0},
    "Consumer":    {"opm": 16.0, "npm": 10.0, "sg": 10.0},
    "Power":       {"opm": 25.0, "npm": 10.0, "sg": 10.0},
    "Infra":       {"opm": 12.0, "npm": 5.0,  "sg": 12.0},
    "Real Estate": {"opm": 25.0, "npm": 12.0, "sg": 15.0},
    "Telecom":     {"opm": 30.0, "npm": 8.0,  "sg": 8.0},
}


def _bench(sector: str) -> dict:
    for k in BENCHMARKS:
        if k.lower() in sector.lower():
            return BENCHMARKS[k]
    return BENCHMARKS["default"]


def _f(v, suf="", dec=1) -> str:
    return f"{round(v, dec)}{suf}" if v is not None else "N/A"


def _trend(lst, n=3) -> str:
    c = [v for v in lst if v is not None]
    return " -> ".join(_f(x) for x in c[-n:]) + f" (last {min(n,len(c))}Y)" if c else "no data"


def score(data: dict) -> dict:
    if "error" in data:
        return {"error": data["error"], "symbol": data.get("symbol", "")}

    d = data["derived"]
    s = data["series"]
    b = _bench(data.get("sector", ""))

    def c(id_, label, result, sc, detail, w=1.0):
        return {"id": id_, "label": label, "result": result, "score": sc, "detail": detail, "weight": w}

    crit = []

    # C1 Sales growth
    sg = d["sales_avg_growth"]
    if sg is None:   crit.append(c("c1","Sales growth >= industry","neutral",0.5,f"Insufficient data. Benchmark: {b['sg']}%"))
    elif sg >= b["sg"]: crit.append(c("c1","Sales growth >= industry","pass",1.0,f"Avg growth {_f(sg,'%')} vs benchmark {b['sg']}%"))
    else:            crit.append(c("c1","Sales growth >= industry","fail",0.0,f"Avg growth {_f(sg,'%')} below benchmark {b['sg']}%"))

    # C2 OPM
    opm = d["opm"]
    if opm is None:      crit.append(c("c2","OPM > industry","neutral",0.5,"OPM unavailable"))
    elif opm > b["opm"]: crit.append(c("c2","OPM > industry","pass",1.0,f"OPM {_f(opm,'%')} vs benchmark {b['opm']}%"))
    else:                crit.append(c("c2","OPM > industry","fail",0.0,f"OPM {_f(opm,'%')} below benchmark {b['opm']}%"))

    # C3 EPS increasing (w 1.5)
    ei = d["eps_increasing"]
    if ei is None: crit.append(c("c3","EPS consistently increasing","neutral",0.5,f"Trend: {_trend(s['eps'])}",1.5))
    elif ei:       crit.append(c("c3","EPS consistently increasing","pass",1.0,f"EPS: {_trend(s['eps'])}",1.5))
    else:          crit.append(c("c3","EPS consistently increasing","fail",0.0,f"EPS not rising: {_trend(s['eps'])}",1.5))

    # C4 NPM
    npm = d["npm"]
    if npm is None:       crit.append(c("c4","Net margin >= industry","neutral",0.5,"NPM unavailable"))
    elif npm >= b["npm"]: crit.append(c("c4","Net margin >= industry","pass",1.0,f"NPM {_f(npm,'%')} vs benchmark {b['npm']}%"))
    else:                 crit.append(c("c4","Net margin >= industry","fail",0.0,f"NPM {_f(npm,'%')} below benchmark {b['npm']}%"))

    # C5 Reserves
    ri = d["reserves_increasing"]
    if ri is None: crit.append(c("c5","Reserves increasing","neutral",0.5,"Insufficient data"))
    elif ri:       crit.append(c("c5","Reserves increasing","pass",1.0,f"Reserves: {_trend(s['reserves'])} Cr"))
    else:          crit.append(c("c5","Reserves increasing","fail",0.0,f"Reserves declining: {_trend(s['reserves'])} Cr"))

    # C6 Debt (w 1.5)
    d2e = d["debt_to_equity"]
    if d2e is None:  crit.append(c("c6","Low / no debt","neutral",0.5,"D/E unavailable",1.5))
    elif d2e <= 0.3: crit.append(c("c6","Low / no debt","pass",1.0,f"D/E = {_f(d2e)} -- very low leverage",1.5))
    elif d2e <= 0.8: crit.append(c("c6","Low / no debt","neutral",0.5,f"D/E = {_f(d2e)} -- moderate debt",1.5))
    else:            crit.append(c("c6","Low / no debt","fail",0.0,f"D/E = {_f(d2e)} -- high leverage",1.5))

    # C7 Cash
    ci = d["cash_increasing"]
    if ci is None: crit.append(c("c7","Cash on BS increasing","neutral",0.5,"Insufficient data"))
    elif ci:       crit.append(c("c7","Cash on BS increasing","pass",1.0,f"Cash: {_trend(s['cash_bs'])} Cr"))
    else:          crit.append(c("c7","Cash on BS increasing","fail",0.0,f"Cash declining: {_trend(s['cash_bs'])} Cr"))

    # C8 Fixed assets (w 0.8)
    fai = d["fixed_assets_increasing"]
    if fai is None: crit.append(c("c8","Fixed assets increasing","neutral",0.5,"Insufficient data",0.8))
    elif fai:       crit.append(c("c8","Fixed assets increasing","pass",1.0,f"Fixed assets: {_trend(s['fixed_assets'])} Cr",0.8))
    else:           crit.append(c("c8","Fixed assets increasing","fail",0.0,f"Fixed assets: {_trend(s['fixed_assets'])} Cr",0.8))

    # C9 Negative NWC (bonus)
    nwc = d["nwc"]
    if d["nwc_negative"]:
        crit.append(c("c9","Negative NWC (bargaining power)","pass",1.5,f"NWC = {_f(nwc)} Cr -- strong supplier terms"))
    else:
        crit.append(c("c9","Negative NWC (bargaining power)","neutral",0.5,f"NWC = {_f(nwc)} Cr -- positive"))

    # C10 CFO (w 2.0)
    cfo_pos = d["cfo_positive"]; cfo_inc = d["cfo_increasing"]; cfo_v = d["last_cfo"]
    if cfo_pos and cfo_inc: crit.append(c("c10","CFO positive & increasing","pass",1.0,f"CFO {_f(cfo_v)} Cr: {_trend(s['cfo'])} Cr",2.0))
    elif cfo_pos:           crit.append(c("c10","CFO positive & increasing","neutral",0.5,f"CFO positive ({_f(cfo_v)} Cr) but not consistently growing",2.0))
    else:                   crit.append(c("c10","CFO positive & increasing","fail",0.0,f"CFO {_f(cfo_v)} Cr -- negative cash flow",2.0))

    # C11 CFI
    cfi_v = d["last_cfi"]
    if d["cfi_negative"]: crit.append(c("c11","CFI negative (investing in growth)","pass",1.0,f"CFI = {_f(cfi_v)} Cr -- investing actively"))
    else:                 crit.append(c("c11","CFI negative (investing in growth)","neutral",0.5,f"CFI = {_f(cfi_v)} Cr -- positive (possible asset sales)"))

    # C12 CFF
    cff_v = d["last_cff"]
    if cff_v is None:  crit.append(c("c12","CFF: debt repayment or growth","neutral",0.5,"CFF unavailable"))
    elif cff_v < 0:    crit.append(c("c12","CFF: debt repayment or growth","pass",1.0,f"CFF = {_f(cff_v)} Cr -- net debt repayment"))
    else:              crit.append(c("c12","CFF: debt repayment or growth","neutral",0.5,f"CFF = {_f(cff_v)} Cr -- raising capital (verify if for growth)"))

    # C13 ROE > ROCE (w 1.5)
    roe = d["roe"]; roce = d["roce"]
    if roe is None or roce is None: crit.append(c("c13","ROE > ROCE","neutral",0.5,f"ROE={_f(roe,'%')} ROCE={_f(roce,'%')}",1.5))
    elif roe > roce:                crit.append(c("c13","ROE > ROCE","pass",1.0,f"ROE {_f(roe,'%')} > ROCE {_f(roce,'%')} -- leverage working for shareholders",1.5))
    else:                           crit.append(c("c13","ROE > ROCE","fail",0.0,f"ROE {_f(roe,'%')} <= ROCE {_f(roce,'%')} -- leverage not adding value",1.5))

    tw = sum(x["weight"] for x in crit)
    ws = sum(x["score"] * x["weight"] for x in crit)
    pct = round((ws / tw) * 100, 1)

    passes   = sum(1 for x in crit if x["result"] == "pass")
    fails    = sum(1 for x in crit if x["result"] == "fail")
    neutrals = sum(1 for x in crit if x["result"] == "neutral")

    if pct >= 78:   verdict, vc = "Strong buy candidate", "green"
    elif pct >= 58: verdict, vc = "Moderate -- dig deeper", "amber"
    elif pct >= 40: verdict, vc = "Weak fundamentals", "red"
    else:           verdict, vc = "Avoid", "red"

    return {
        "symbol": data["symbol"], "name": data["name"], "sector": data.get("sector",""),
        "screener_url": data["url"], "score_pct": pct,
        "passes": passes, "fails": fails, "neutrals": neutrals,
        "verdict": verdict, "verdict_color": vc,
        "criteria": crit, "top_ratios": data.get("top_ratios", {}), "benchmark": b,
    }
