#!/usr/bin/env python3
"""Render the asymptotic parser study (Nornic vs ANTLR) from scaling.json / tail.json.

Usage: parser_scaling_report.py <dir>   (dir holds scaling.json, optional tail.json)
Writes: <dir>/report.md and <dir>/*.svg. Stdlib only.

For each (family, parser, mode) it fits  log y = a + k log x  against the ANTLR
token count of the input and reports the growth exponent k (1 = linear, 2 =
quadratic) with R^2. allocs/op and B/op are hardware independent; ns/op is not.
"""
import json, math, sys, os
from collections import defaultdict

FIT_POINTS = 4  # fit the largest sizes: fixed per-call overhead dominates small n and flattens the slope
COL = {"nornic": "#1f77b4", "antlr": "#d95f02"}


def fit(xs, ys):
    pts = [(math.log(x), math.log(y)) for x, y in zip(xs, ys) if x > 0 and y > 0]
    n = len(pts)
    if n < 3:
        return None
    mx = sum(p[0] for p in pts) / n
    my = sum(p[1] for p in pts) / n
    sxx = sum((p[0] - mx) ** 2 for p in pts)
    sxy = sum((p[0] - mx) * (p[1] - my) for p in pts)
    syy = sum((p[1] - my) ** 2 for p in pts)
    if sxx == 0:
        return None
    k = sxy / sxx
    r2 = (sxy * sxy) / (sxx * syy) if syy > 0 else 1.0
    return k, r2, my - k * mx


def human(v):
    for unit, d in (("G", 1e9), ("M", 1e6), ("k", 1e3)):
        if abs(v) >= d:
            return f"{v/d:.3g}{unit}"
    return f"{v:.3g}"


def panel(ax, ay, w, h, title, series, ylabel):
    """series: {parser: [(x,y)]} log-log panel."""
    allp = [p for s in series.values() for p in s if p[0] > 0 and p[1] > 0]
    if not allp:
        return ""
    lx = [math.log10(p[0]) for p in allp]
    ly = [math.log10(p[1]) for p in allp]
    x0, x1 = min(lx), max(lx)
    y0, y1 = math.floor(min(ly)), math.ceil(max(ly))
    if y1 == y0:
        y1 += 1
    if x1 == x0:
        x1 += 1
    pl, pr, pt, pb = 46, 8, 20, 22
    iw, ih = w - pl - pr, h - pt - pb
    sx = lambda v: ax + pl + (math.log10(v) - x0) / (x1 - x0) * iw
    sy = lambda v: ay + pt + ih - (math.log10(v) - y0) / (y1 - y0) * ih
    o = [f'<text x="{ax+pl}" y="{ay+13}" font-size="11" font-weight="600" fill="currentColor">{title}</text>']
    o.append(f'<rect x="{ax+pl}" y="{ay+pt}" width="{iw}" height="{ih}" fill="none" stroke="#8884"/>')
    for d in range(int(y0), int(y1) + 1):
        yy = ay + pt + ih - (d - y0) / (y1 - y0) * ih
        o.append(f'<line x1="{ax+pl}" x2="{ax+pl+iw}" y1="{yy:.1f}" y2="{yy:.1f}" stroke="#8882"/>')
        o.append(f'<text x="{ax+pl-4}" y="{yy+3:.1f}" font-size="9" text-anchor="end" fill="currentColor">{human(10**d)}</text>')
    for v in (x0, x1):
        o.append(f'<text x="{ax+pl+(v-x0)/(x1-x0)*iw:.1f}" y="{ay+h-8}" font-size="9" text-anchor="middle" fill="currentColor">{human(10**v)}</text>')
    for name, pts in series.items():
        pts = [p for p in pts if p[0] > 0 and p[1] > 0]
        if not pts:
            continue
        d = " ".join(f"{sx(x):.1f},{sy(y):.1f}" for x, y in pts)
        o.append(f'<polyline points="{d}" fill="none" stroke="{COL[name]}" stroke-width="1.8"/>')
        for x, y in pts:
            o.append(f'<circle cx="{sx(x):.1f}" cy="{sy(y):.1f}" r="2.2" fill="{COL[name]}"/>')
    return "\n".join(o)


def grid_svg(path, fams, data, mode, metric, ylabel):
    cols = 3
    pw, ph = 300, 190
    rows_n = math.ceil(len(fams) / cols)
    W, H = cols * pw, rows_n * ph + 34
    o = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" font-family="system-ui,sans-serif" color="#222">',
         f'<rect width="{W}" height="{H}" fill="white"/>',
         f'<text x="8" y="16" font-size="13" font-weight="700">{ylabel} vs input tokens (log-log), {mode}</text>',
         f'<g font-size="11"><rect x="{W-170}" y="6" width="10" height="10" fill="{COL["nornic"]}"/><text x="{W-156}" y="15">Nornic</text>'
         f'<rect x="{W-100}" y="6" width="10" height="10" fill="{COL["antlr"]}"/><text x="{W-86}" y="15">ANTLR</text></g>']
    for i, f in enumerate(fams):
        series = {}
        for parser in ("nornic", "antlr"):
            rs = sorted(data.get((f, parser, mode), []), key=lambda r: r["tokens"])
            series[parser] = [(r["tokens"], r[metric]) for r in rs]
        o.append(panel((i % cols) * pw, 28 + (i // cols) * ph, pw, ph, f, series, ylabel))
    o.append("</svg>")
    open(path, "w").write("\n".join(o))


def table(headers, rows):
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


def verdict(k):
    if k is None:
        return "n/a"
    if k < 0.2:
        return "O(1)"
    if k < 1.25:
        return "~O(n)"
    if k < 1.75:
        return "~O(n log n)/superlinear"
    return "~O(n^2) or worse"


def main(d):
    sc = json.load(open(os.path.join(d, "scaling.json")))
    rows = sc["rows"]
    fams = [f["name"] for f in sc["families"]]
    desc = {f["name"]: f["desc"] for f in sc["families"]}
    data = defaultdict(list)
    errors = []
    for r in rows:
        if r["status"] != "ok":
            errors.append(r)
            continue
        data[(r["family"], r["parser"], r["mode"])].append(r)

    md = ["# Parser asymptotic study: Nornic vs ANTLR", "",
          f"Machine: {sc['goos']}/{sc['goarch']}, {sc['cpus']} CPUs, {sc['go']}. Sizes n = {sc['sizes']}.", "",
          "Each family grows one query dimension n. Cost is fit as `y = c * tokens^k` on a log-log scale; "
          "`k` is the growth exponent (1 = linear, 2 = quadratic) and R² is fit quality. Fits use the largest 4 sizes, where per-call constant overhead no longer flattens the slope. "
          "**allocs/op and B/op do not depend on the hardware; ns/op does.** Tokens come from the ANTLR lexer, used as a "
          "common input-size measure for both parsers.", ""]

    exps = {}
    for mode in ("parse", "validate"):
        md.append(f"## Growth exponents, mode = {mode}\n")
        trs = []
        for f in fams:
            for parser in ("nornic", "antlr"):
                rs = sorted(data.get((f, parser, mode), []), key=lambda r: r["tokens"])
                if not rs:
                    trs.append([f, parser, "failed", "", "", "", ""])
                    continue
                cells = []
                tail = rs[-FIT_POINTS:]
                for m in ("allocs_per_op", "bytes_per_op", "ns_per_op"):
                    if all(r[m] == 0 for r in tail):
                        exps[(f, parser, mode, m)] = 0.0
                        cells.append("0 allocs (constant)")
                        continue
                    ft = fit([r["tokens"] for r in tail], [r[m] for r in tail])
                    exps[(f, parser, mode, m)] = ft[0] if ft else None
                    cells.append(f"{ft[0]:.2f} (R²={ft[1]:.3f})" if ft else "n/a")
                trs.append([f, parser, *cells, verdict(exps[(f, parser, mode, "allocs_per_op")])])
        md.append(table(["family", "parser", "allocs k", "bytes k", "time k", "allocs growth"], trs))
        md.append("")

    md.append("## Cost at the largest size (parse mode)\n")
    trs = []
    for f in fams:
        a = sorted(data.get((f, "antlr", "parse"), []), key=lambda r: r["tokens"])
        n = sorted(data.get((f, "nornic", "parse"), []), key=lambda r: r["tokens"])
        if a and n:
            a, n = a[-1], n[-1]
            trs.append([f, n["tokens"], f'{n["allocs_per_op"]:,}', f'{a["allocs_per_op"]:,}',
                        f'{a["allocs_per_op"]/max(n["allocs_per_op"],1):.0f}x',
                        human(n["bytes_per_op"]) + "B", human(a["bytes_per_op"]) + "B",
                        f'{n["ns_per_op"]/1e3:.1f}µs', f'{a["ns_per_op"]/1e3:.1f}µs',
                        f'{a["ns_per_op"]/max(n["ns_per_op"],1):.0f}x'])
    md.append(table(["family", "tokens", "Nornic allocs", "ANTLR allocs", "ratio", "Nornic B", "ANTLR B",
                     "Nornic time", "ANTLR time", "ratio"], trs))
    md.append("")

    for mode in ("parse", "validate"):
        for metric, label in (("allocs_per_op", "allocs per op"), ("bytes_per_op", "bytes per op"), ("ns_per_op", "ns per op")):
            fn = f"{mode}_{metric}.svg"
            grid_svg(os.path.join(d, fn), fams, data, mode, metric, label)
            md.append(f"![{mode} {label}]({fn})\n")

    tail_path = os.path.join(d, "tail.json")
    if os.path.exists(tail_path):
        tail = json.load(open(tail_path))["rows"]
        md.append("## Tail latency and GC (parse mode, n=32, fixed input)\n")
        by = defaultdict(dict)
        for r in tail:
            by[r["query"]][r["parser"]] = r
        trs = []
        for q, ps in by.items():
            if "nornic" in ps and "antlr" in ps:
                for p in ("nornic", "antlr"):
                    r = ps[p]
                    trs.append([q, p, f'{r["p50_ns"]/1e3:.1f}', f'{r["p95_ns"]/1e3:.1f}', f'{r["p99_ns"]/1e3:.1f}',
                                f'{r["max_ns"]/1e3:.0f}', f'{r["max_ns"]/max(r["p50_ns"],1):.0f}x',
                                r["num_gc"], f'{r["gc_pause_total_ns"]/1e6:.2f}'])
        md.append(table(["input", "parser", "p50 µs", "p95 µs", "p99 µs", "max µs", "max/p50", "GCs", "GC pause ms"], trs))
        md.append("")

    if errors:
        md.append("## Inputs one parser rejected (excluded from fits)\n")
        md.append(table(["family", "n", "parser", "mode", "status"],
                        [[e["family"], e["n"], e["parser"], e["mode"], e["status"][:100]] for e in errors]))
        md.append("")

    md.append("""## How to read this, and its limits

- **What this proves.** The exponent `k` and the allocation counts are properties of the algorithm and its memory behaviour, not of this CPU. A parser whose allocs/op grow as tokens^1 and whose constant is 30x lower is cheaper on any hardware. ns/op is reported but is the weakest evidence.
- **What this does not prove.** Fits are empirical over n = 8..max, not a proof of asymptotic class. Confirm with a derivation from the algorithm (e.g. each token consumed once, no backtracking) before claiming O(n) in print.
- **The parsers do different amounts of work.** `nornic parse` (`ASTBuilder.Build`) is a clause splitter plus string-based clause parsing, and `nornic validate` is a set of scanners. ANTLR runs a full grammar (ALL(*)) and, in parse mode, builds a complete parse tree. A big ratio partly reflects "less work", not only "better implementation". The fair statement is the cost of getting a usable structure out of the query, plus the exponents. Check `Clauses` and error rejection parity before claiming equivalence.
- **Caches.** The Nornic validate path memoises per-executor; the harness clears it before each call. ANTLR's DFA cache stays warm (as in a long-running server), which favours ANTLR.
- **Scope.** This is parse time only. End-to-end query latency (storage, planning, execution) is dominated by other costs, so parser results should not be presented as database speedups.
""")
    open(os.path.join(d, "report.md"), "w").write("\n".join(md))
    print("wrote", os.path.join(d, "report.md"))


if __name__ == "__main__":
    main(sys.argv[1])
