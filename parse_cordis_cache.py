#!/usr/bin/env python3
"""
parse_cordis_cache.py
═════════════════════
Parses CORDIS XLSX files already downloaded in cordis_cache/,
filters against the master keyword list, deduplicates across
programmes, and writes cordis_textile_projects.json compatible
with the combined network visualisation.

Expected directory layout:
  cordis_cache/
    cordis_HorizonEurope/
      project.xlsx
      organization.xlsx
    cordis_H2020/
      project.xlsx
      organization.xlsx

Usage
  python3 parse_cordis_cache.py
  python3 parse_cordis_cache.py --cache-dir ~/cordis_cache --out results.json
"""

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

try:
    import openpyxl
except ImportError:
    sys.exit("openpyxl is required:  pip install openpyxl --break-system-packages")

# ─── Master keyword list ─────────────────────────────────────────────────────
KEYWORDS = [
    "textile", "textiel", "fashion", "garment",
    "wool", "wol", "cotton", "katoen",
    "fiber", "fibre", "vezel",
    "weven", "kleding",
    "zijde", "polyester", "denim", "leather",
    "fabric", "dye",
    "recycled textile", "circular textile", "circular fashion",
    "circulair textiel", "textiel recycling",
    "smart textile",
    "duurzame mode",
    "lifecycle", "textile value chain",
    "hergebruik", "biobased", "wearable",
    "textile supply chain", "waardeketen", "levenscyclus",
    "biomaterial", "coating", "hemp", "hennep",
]

# ─── Programme registry ──────────────────────────────────────────────────────
PROGRAMMES = [
    {
        "key":       "HorizonEurope",
        "label":     "Horizon Europe",
        "subdir":    "cordis_HorizonEurope",
    },
    {
        "key":       "H2020",
        "label":     "Horizon 2020",
        "subdir":    "cordis_H2020",
    },
]


def match_keywords(text: str) -> list[str]:
    t = (text or "").lower()
    return [kw for kw in KEYWORDS if kw in t]


def safe(val) -> str:
    return str(val).strip() if val is not None else ""


def read_xlsx(path: Path) -> list[dict]:
    """Return list of row dicts from first sheet of an XLSX file."""
    print(f"    reading {path.name} ({path.stat().st_size / 1_048_576:.1f} MB)…",
          flush=True)
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    wb.close()
    if not rows:
        return []
    headers = [
        str(h).strip() if h is not None else f"col_{i}"
        for i, h in enumerate(rows[0])
    ]
    return [dict(zip(headers, row)) for row in rows[1:]]


def parse_programme(prog: dict, cache_dir: Path) -> tuple[list[dict], int]:
    """
    Parse one CORDIS programme directory.
    Returns (matched_projects, total_project_count).
    """
    subdir = cache_dir / prog["subdir"]
    if not subdir.exists():
        print(f"  WARNING: {subdir} not found — skipping {prog['label']}")
        return [], 0

    # ── Load projects ─────────────────────────────────────────────────────────
    proj_path = subdir / "project.xlsx"
    if not proj_path.exists():
        print(f"  WARNING: {proj_path} not found — skipping {prog['label']}")
        return [], 0

    print(f"\n  {prog['label']}")
    print(f"  {'─' * 50}")
    projects = read_xlsx(proj_path)
    total = len(projects)
    print(f"    {total:,} project rows")

    # ── Load organisations ────────────────────────────────────────────────────
    org_path = subdir / "organization.xlsx"
    org_index: dict[str, list] = {}
    if org_path.exists():
        orgs = read_xlsx(org_path)
        print(f"    {len(orgs):,} organisation rows")
        for o in orgs:
            pid = safe(
                o.get("projectID") or o.get("projectId") or
                o.get("project_id") or ""
            )
            if pid:
                org_index.setdefault(pid, []).append(o)
        print(f"    {len(org_index):,} projects have organisation records")
    else:
        print(f"    WARNING: organization.xlsx not found in {subdir}")

    # ── Filter by keywords ────────────────────────────────────────────────────
    matched: list[dict] = []
    for p in projects:
        pid   = safe(
            p.get("id") or p.get("projectID") or p.get("rcn") or ""
        )
        title = safe(p.get("title") or "")
        obj   = safe(
            p.get("objective") or p.get("teaser") or p.get("summary") or ""
        )
        kws   = safe(p.get("keywords") or p.get("topics") or "")

        hits = match_keywords(title + " " + obj + " " + kws)
        if not hits:
            continue

        proj_orgs = org_index.get(pid, [])
        coord = next(
            (o for o in proj_orgs
             if "coord" in safe(o.get("role", "")).lower()),
            proj_orgs[0] if proj_orgs else None,
        )

        # Compute total EC grant for this project
        # CORDIS uses European decimal notation: "1234567,89" = €1,234,567.89
        ec_raw = safe(
            p.get("ecMaxContribution") or p.get("ecContribution") or ""
        )
        try:
            # Replace comma (European decimal sep) with dot; strip spaces
            ec_grant = float(str(ec_raw).replace(" ", "").replace(",", "."))
        except ValueError:
            ec_grant = 0.0

        matched.append({
            "project_id":       pid,
            "title_en":         title,
            "acronym":          safe(p.get("acronym") or ""),
            "funding_scheme":   safe(
                p.get("fundingScheme") or
                p.get("frameworkProgramme") or
                prog["label"]
            ),
            "programme":        prog["label"],
            "start_date":       safe(p.get("startDate") or ""),
            "end_date":         safe(p.get("endDate") or ""),
            "status":           safe(p.get("status") or ""),
            "total_cost":       safe(p.get("totalCost") or ""),
            "ec_grant":         ec_raw,
            "ec_grant_num":     ec_grant,
            "objective":        obj[:1200],
            "topics":           kws,
            "_pi_institution":  safe(coord.get("name") or "")    if coord else "",
            "_pi_country":      safe(coord.get("country") or "") if coord else "",
            "_pi_name":         "",
            "_matched_terms":   hits,
            "project_members":  [
                {
                    "organisation": safe(o.get("name") or ""),
                    "country":      safe(o.get("country") or ""),
                    "role":         safe(o.get("role") or ""),
                    "activityType": safe(o.get("activityType") or ""),
                }
                for o in proj_orgs
            ],
            "project_page_url": (
                f"https://cordis.europa.eu/project/id/{pid}" if pid else ""
            ),
        })

    print(f"    {len(matched):,} projects matched keywords")
    return matched, total


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache-dir", default="./cordis_cache")
    ap.add_argument("--out",       default="cordis_textile_projects.json")
    args = ap.parse_args()

    cache_dir = Path(args.cache_dir)
    if not cache_dir.exists():
        sys.exit(f"Cache dir not found: {cache_dir}")

    all_projects: list[dict] = []
    seen_ids: set[str] = set()
    totals: dict[str, int] = {}

    for prog in PROGRAMMES:
        matched, total = parse_programme(prog, cache_dir)
        totals[prog["key"]] = total

        for p in matched:
            key = p["project_id"]
            if key and key in seen_ids:
                continue
            if key:
                seen_ids.add(key)
            all_projects.append(p)

    total_he    = totals.get("HorizonEurope", 0)
    total_h2020 = totals.get("H2020", 0)
    total_all   = total_he + total_h2020

    # ── Write JSON ────────────────────────────────────────────────────────────
    output = {
        "meta": {
            "total_he":           total_he,
            "total_h2020":        total_h2020,
            "total_cordis":       total_all,
            "matched_projects":   len(all_projects),
        },
        "projects": all_projects,
    }

    out_path = Path(args.out)
    out_path.write_text(
        json.dumps(output, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    total_ec = sum(p.get("ec_grant_num", 0) for p in all_projects)

    print(f"\n  {'═' * 50}")
    print(f"  Horizon Europe projects   : {total_he:>8,}")
    print(f"  Horizon 2020 projects     : {total_h2020:>8,}")
    print(f"  Total CORDIS projects     : {total_all:>8,}")
    print(f"  Keyword-matched           : {len(all_projects):>8,}")
    print(f"  Total EC grant (matched)  : €{total_ec:,.0f}")
    print(f"  Written → {out_path}\n")

    # Keyword summary
    kw_counts = Counter(
        kw for p in all_projects for kw in p["_matched_terms"]
    )
    print("Keyword hits across all matched projects:")
    for kw, n in kw_counts.most_common(25):
        print(f"  {n:5d}  {kw}")


if __name__ == "__main__":
    main()
