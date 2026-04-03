#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["requests", "tqdm", "openpyxl"]
# ///
"""
fetch_cordis_bulk.py
════════════════════
Downloads the official CORDIS open-data ZIP archives for Horizon Europe,
H2020 (and optionally FP7), filters them against the textile/fashion keyword
list, and writes a JSON file compatible with the network visualisations.

CORRECT FILE STRUCTURE
  Each ZIP contains two XLSX files:
    cordis-HORIZONprojects-xlsx/
      project.xlsx       ← one row per project
      organization.xlsx  ← one row per participating organisation

  project.xlsx columns include:
    id, acronym, title, startDate, endDate, fundingScheme, subCall,
    objective, totalCost, ecMaxContribution, status, ...

  organization.xlsx columns:
    projectID, organisationID, name, shortName, country, activityType,
    role, ecContribution, totalCost, ...

DOWNLOAD URLS (verified 2026-04)
  Horizon Europe : https://cordis.europa.eu/data/cordis-HORIZONprojects-xlsx.zip
  H2020          : https://cordis.europa.eu/data/cordis-h2020projects-xlsx.zip
  FP7            : https://cordis.europa.eu/data/cordis-fp7projects-xlsx.zip

USAGE
  uv run fetch_cordis_bulk.py
  uv run fetch_cordis_bulk.py --out cordis_textile.json
  uv run fetch_cordis_bulk.py --cache-dir ~/cordis_cache --include-fp7
  uv run fetch_cordis_bulk.py --no-h2020        # Horizon Europe only

  Cached ZIPs are reused if less than 7 days old (--max-age to change).

OUTPUT FORMAT
  JSON array matching the nwo_textile_raw_network.html viewer schema:
    project_id, title_en, funding_scheme, start_date, end_date,
    _pi_institution, _pi_country, _matched_terms,
    project_members, project_page_url, objective
"""

import argparse
import io
import json
import sys
import zipfile
from datetime import datetime, timedelta
from pathlib import Path

try:
    import openpyxl
    import requests
    from tqdm import tqdm
except ImportError:
    sys.exit("Run with:  uv run fetch_cordis_bulk.py")


# ─── Keyword list ────────────────────────────────────────────────────────────
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
    "lifecycle", "value chain",
    "hergebruik", "biobased", "wearable",
    "supply chain", "waardeketen", "levenscyclus",
    "biomaterial", "coating", "hemp", "hennep",
]

# ─── Programme registry ──────────────────────────────────────────────────────
SOURCES = {
    "HorizonEurope": {
        "zip_url":   "https://cordis.europa.eu/data/cordis-HORIZONprojects-xlsx.zip",
        "zip_dir":   "cordis-HORIZONprojects-xlsx",
        "programme": "Horizon Europe",
    },
    "H2020": {
        "zip_url":   "https://cordis.europa.eu/data/cordis-h2020projects-xlsx.zip",
        "zip_dir":   "cordis-h2020projects-xlsx",
        "programme": "Horizon 2020",
    },
    "FP7": {
        "zip_url":   "https://cordis.europa.eu/data/cordis-fp7projects-xlsx.zip",
        "zip_dir":   "cordis-fp7projects-xlsx",
        "programme": "FP7",
    },
}


# ─── Helpers ─────────────────────────────────────────────────────────────────

def is_fresh(path: Path, max_age_days: int) -> bool:
    if not path.exists():
        return False
    age = datetime.now() - datetime.fromtimestamp(path.stat().st_mtime)
    return age < timedelta(days=max_age_days)


def download_zip(url: str, cache_path: Path, label: str, max_age: int) -> Path:
    """Download url to cache_path (with progress bar). Returns cache_path."""
    if is_fresh(cache_path, max_age):
        size_mb = cache_path.stat().st_size / 1_048_576
        print(f"  [cache]    {label}  ({size_mb:.0f} MB, < {max_age}d old)")
        return cache_path

    print(f"  [download] {label}")
    print(f"             {url}")
    resp = requests.get(url, stream=True, timeout=300)
    resp.raise_for_status()

    total = int(resp.headers.get("content-length", 0))
    with cache_path.open("wb") as fh, \
         tqdm(total=total, unit="B", unit_scale=True,
              desc=cache_path.name, ncols=80, leave=False) as bar:
        for chunk in resp.iter_content(chunk_size=1 << 17):
            fh.write(chunk)
            bar.update(len(chunk))

    size_mb = cache_path.stat().st_size / 1_048_576
    print(f"             saved → {cache_path}  ({size_mb:.0f} MB)")
    return cache_path


def xlsx_rows(zip_path: Path, inner_dir: str, filename: str) -> list[dict]:
    """
    Open zip_path, extract inner_dir/filename XLSX, return list of row dicts.
    Tries both with and without the inner_dir prefix in case of ZIP layout variation.
    """
    candidates = [
        f"{inner_dir}/{filename}",
        filename,
        f"{inner_dir}/{inner_dir}/{filename}",
    ]
    with zipfile.ZipFile(zip_path) as zf:
        names = zf.namelist()
        target = None
        for c in candidates:
            if c in names:
                target = c
                break
        if target is None:
            # fuzzy match — find any path ending with the filename
            matches = [n for n in names if n.endswith(filename)]
            if not matches:
                raise FileNotFoundError(
                    f"{filename} not found in {zip_path.name}\n"
                    f"  Contents: {names[:20]}"
                )
            target = matches[0]

        print(f"    reading  {target}", flush=True)
        with zf.open(target) as xlsx_file:
            wb = openpyxl.load_workbook(xlsx_file, read_only=True, data_only=True)
            ws = wb.active
            rows = list(ws.iter_rows(values_only=True))

    if not rows:
        return []
    headers = [str(h).strip() if h is not None else f"col_{i}"
               for i, h in enumerate(rows[0])]
    return [dict(zip(headers, row)) for row in rows[1:]]


def match_keywords(text: str) -> list[str]:
    t = (text or "").lower()
    return [kw for kw in KEYWORDS if kw in t]


def safe(val) -> str:
    return str(val).strip() if val is not None else ""


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out",         default="cordis_textile_projects.json")
    ap.add_argument("--cache-dir",   default="./cordis_cache")
    ap.add_argument("--no-h2020",    action="store_true")
    ap.add_argument("--include-fp7", action="store_true")
    ap.add_argument("--max-age",     type=int, default=7,
                    help="Re-download cached ZIPs older than N days")
    args = ap.parse_args()

    cache_dir = Path(args.cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)

    programmes = ["HorizonEurope"]
    if not args.no_h2020:
        programmes.append("H2020")
    if args.include_fp7:
        programmes.append("FP7")

    all_matched = []

    for prog in programmes:
        src = SOURCES[prog]
        print(f"\n{'═'*62}")
        print(f"  {src['programme']}")
        print(f"{'═'*62}")

        zip_path = cache_dir / f"cordis_{prog}.zip"
        download_zip(src["zip_url"], zip_path, src["programme"], args.max_age)

        # ── Load projects ──────────────────────────────────────────────────
        print(f"  Loading project.xlsx…", flush=True)
        try:
            projects = xlsx_rows(zip_path, src["zip_dir"], "project.xlsx")
        except FileNotFoundError as e:
            print(f"  ERROR: {e}")
            continue
        print(f"  {len(projects):,} project rows loaded")

        # ── Load organisations ─────────────────────────────────────────────
        print(f"  Loading organization.xlsx…", flush=True)
        try:
            orgs = xlsx_rows(zip_path, src["zip_dir"], "organization.xlsx")
        except FileNotFoundError as e:
            print(f"  WARNING: {e} — institution data will be empty")
            orgs = []

        # Build org lookup: projectID (str) → list of org dicts
        org_index: dict[str, list] = {}
        for o in orgs:
            pid = safe(o.get("projectID") or o.get("projectId") or "")
            if pid:
                org_index.setdefault(pid, []).append(o)
        print(f"  {len(org_index):,} projects have organisation records")

        # ── Filter projects ────────────────────────────────────────────────
        print(f"  Filtering by keyword…", flush=True)
        matched = 0
        for p in tqdm(projects, desc="  scanning", ncols=80, leave=False):
            pid   = safe(p.get("id") or p.get("projectID") or p.get("rcn") or "")
            title = safe(p.get("title") or "")
            obj   = safe(p.get("objective") or p.get("teaser") or p.get("summary") or "")

            hits = match_keywords(title + " " + obj)
            if not hits:
                continue

            matched += 1
            proj_orgs = org_index.get(pid, [])

            # Coordinator = lead institution
            coord = next(
                (o for o in proj_orgs if "coord" in safe(o.get("role", "")).lower()),
                proj_orgs[0] if proj_orgs else None,
            )

            all_matched.append({
                "project_id":       pid,
                "title_en":         title,
                "acronym":          safe(p.get("acronym") or ""),
                "funding_scheme":   safe(p.get("fundingScheme") or p.get("frameworkProgramme") or src["programme"]),
                "programme":        src["programme"],
                "start_date":       safe(p.get("startDate") or ""),
                "end_date":         safe(p.get("endDate") or ""),
                "total_cost":       safe(p.get("totalCost") or ""),
                "ec_contribution":  safe(p.get("ecMaxContribution") or p.get("ecContribution") or ""),
                "status":           safe(p.get("status") or ""),
                "objective":        obj[:1200],
                "_pi_institution":  safe(coord.get("name") or "") if coord else "",
                "_pi_country":      safe(coord.get("country") or "") if coord else "",
                "_pi_name":         "",   # not in bulk export
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
                "project_page_url": f"https://cordis.europa.eu/project/id/{pid}",
            })

        print(f"  ✓ {matched:,} projects matched keywords")

    # ── Deduplicate ───────────────────────────────────────────────────────────
    seen: set[str] = set()
    deduped = []
    for p in all_matched:
        key = p["project_id"]
        if key and key not in seen:
            seen.add(key)
            deduped.append(p)

    print(f"\n{'─'*62}")
    print(f"  Total matched (all programmes) : {len(all_matched):,}")
    if len(deduped) < len(all_matched):
        print(f"  After deduplication            : {len(deduped):,}")

    # ── Write output ──────────────────────────────────────────────────────────
    out_path = Path(args.out)
    out_path.write_text(
        json.dumps(deduped, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"\n  ✓ Written → {out_path}")
    print()

    # Keyword summary
    from collections import Counter
    kw_counts = Counter(kw for p in deduped for kw in p["_matched_terms"])
    print("Matched keywords:")
    for kw, n in kw_counts.most_common(20):
        print(f"  {n:5d}  {kw}")


if __name__ == "__main__":
    main()
