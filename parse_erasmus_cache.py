#!/usr/bin/env python3
"""
parse_erasmus_cache.py
══════════════════════
Parses every Erasmus+ CSV file already downloaded in erasmus_cache/,
filters against the master keyword list, deduplicates, and writes
erasmus_textile_projects.json compatible with the network visualisation.

Usage
  python3 parse_erasmus_cache.py
  python3 parse_erasmus_cache.py --cache-dir ~/my_cache --out results.json
"""

import argparse
import csv
import json
import re
import sys
from collections import Counter
from pathlib import Path

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
    "textile lifecycle", "textile value chain",
    "hergebruik", "biobased", "wearable",
    "textile supply chain", "waardeketen", "levenscyclus",
    "biomaterial", "coating", "hemp", "hennep",
]

# ─── Known CSV column names ───────────────────────────────────────────────────
# Confirmed from actual files — no guessing needed
COL_PROGRAMME   = "Programme"
COL_KEY_ACTION  = "Key Action"
COL_ACTION_TYPE = "Action Type"
COL_CALL_YEAR   = "Call year"
COL_ID          = "Project Identifier"
COL_TITLE       = "Project Title"
COL_TOPICS      = "Topics"
COL_SUMMARY     = "Project Summary"
COL_STATUS      = "Project Status"
COL_GRANT       = "EU Grant award in euros (This amount represents the grant awarded after the selection stage and is indicative. Please note that any changes made during or after the project's lifetime will not be reflected here.)"
COL_WEBSITE     = "Project Website"
COL_RESULTS_URL = "Results Platform Project Card"
COL_COUNTRIES   = "Participating countries"
COL_COORD_NAME  = "Coordinating organisation name"
COL_COORD_TYPE  = "Coordinating organisation type"
COL_COORD_ADDR  = "Coordinator's address"
COL_COORD_REGION= "Coordinator's region"
COL_COORD_CTRY  = "Coordinator's country"
COL_COORD_WEB   = "Coordinator's website"
# Partners: "Partner 1 name", "Partner 1 organisation type",
#           "Partner 1 address", "Partner 1 region",
#           "Partner 1 country", "Partner 1 website"  … up to 38


def match_keywords(text: str) -> list[str]:
    t = text.lower()
    return [kw for kw in KEYWORDS if kw in t]


def safe(val) -> str:
    return str(val).strip() if val is not None else ""


def extract_partners(row: dict) -> list[dict]:
    """Extract all numbered partner columns into a list of dicts."""
    partners = []
    # Coordinator first
    coord = safe(row.get(COL_COORD_NAME))
    if coord:
        partners.append({
            "organisation": coord,
            "orgType":      safe(row.get(COL_COORD_TYPE)),
            "country":      safe(row.get(COL_COORD_CTRY)),
            "region":       safe(row.get(COL_COORD_REGION)),
            "role":         "Coordinator",
        })
    # Numbered partners
    for i in range(1, 39):
        name = safe(row.get(f"Partner {i} name"))
        if not name:
            break   # partners are consecutive; stop at first empty
        partners.append({
            "organisation": name,
            "orgType":      safe(row.get(f"Partner {i} organisation type")),
            "country":      safe(row.get(f"Partner {i} country")),
            "region":       safe(row.get(f"Partner {i} region")),
            "role":         "Partner",
        })
    return partners


def action_label(filename: str) -> str:
    """Derive a short label from the CSV filename, e.g. 'KA2 2014-2020'."""
    stem = Path(filename).stem  # strip .csv
    # Remove date stamp at end: _2026-04-02
    stem = re.sub(r'_\d{4}-\d{2}-\d{2}$', '', stem)
    # Strip common prefix
    stem = re.sub(r'^ErasmusPlus_', '', stem)
    # Map known patterns
    m = re.search(r'(KA\d+)_(\d{4}(?:-\d{4})?)', stem)
    if m:
        return f"{m.group(1)} {m.group(2)}"
    if 'JeanMonnet' in stem:
        return 'Jean Monnet'
    if 'Sports' in stem:
        return 'Sports'
    if 'KA3' in stem:
        return 'KA3 2014-2020'
    return stem[:40]


def parse_file(path: Path) -> list[dict]:
    """Parse one Erasmus+ CSV and return matched project dicts."""
    label = action_label(path.name)
    results = []

    with path.open(encoding="utf-8-sig", errors="replace", newline="") as fh:
        reader = csv.DictReader(fh)   # comma-delimited (confirmed)
        for row in reader:
            title   = safe(row.get(COL_TITLE))
            summary = safe(row.get(COL_SUMMARY))
            topics  = safe(row.get(COL_TOPICS))

            hits = match_keywords(title + " " + summary + " " + topics)
            if not hits:
                continue

            pid         = safe(row.get(COL_ID))
            coord_name  = safe(row.get(COL_COORD_NAME))
            coord_ctry  = safe(row.get(COL_COORD_CTRY))
            results_url = safe(row.get(COL_RESULTS_URL))

            results.append({
                "project_id":       pid,
                "title_en":         title,
                "funding_scheme":   label,
                "programme":        "Erasmus+",
                "key_action":       safe(row.get(COL_KEY_ACTION)),
                "action_type":      safe(row.get(COL_ACTION_TYPE)),
                "call_year":        safe(row.get(COL_CALL_YEAR)),
                "start_date":       "",   # not in Erasmus+ CSV
                "end_date":         "",
                "status":           safe(row.get(COL_STATUS)),
                "ec_grant":         safe(row.get(COL_GRANT)),
                "objective":        summary[:1200],
                "topics":           topics,
                "participating_countries": safe(row.get(COL_COUNTRIES)),
                "_pi_institution":  coord_name,
                "_pi_country":      coord_ctry,
                "_pi_name":         "",   # not published
                "_matched_terms":   hits,
                "project_members":  extract_partners(row),
                "project_page_url": results_url or (
                    f"https://erasmus-plus.ec.europa.eu/projects/search/project/{pid}"
                    if pid else ""
                ),
                "project_website":  safe(row.get(COL_WEBSITE)),
            })

    return results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache-dir", default="./erasmus_cache")
    ap.add_argument("--out",       default="erasmus_textile_projects.json")
    args = ap.parse_args()

    cache_dir = Path(args.cache_dir)
    if not cache_dir.exists():
        sys.exit(f"Cache dir not found: {cache_dir}")

    csv_files = sorted(cache_dir.glob("*.csv"))
    if not csv_files:
        sys.exit(f"No CSV files found in {cache_dir}")

    print(f"\nFound {len(csv_files)} CSV files in {cache_dir}\n")

    all_projects: list[dict] = []
    seen_ids: set[str] = set()
    file_counts: dict[str, int] = {}

    for path in csv_files:
        label = action_label(path.name)
        size_mb = path.stat().st_size / 1_048_576
        matches = parse_file(path)

        # Deduplicate across files by Project Identifier
        before = len(all_projects)
        for p in matches:
            key = p["project_id"]
            if key and key in seen_ids:
                continue
            if key:
                seen_ids.add(key)
            all_projects.append(p)

        added = len(all_projects) - before
        file_counts[label] = added
        print(f"  {label:30s}  {added:4d} matches  ({size_mb:5.0f} MB)")

    # ── Write JSON ────────────────────────────────────────────────────────────
    out_path = Path(args.out)
    out_path.write_text(
        json.dumps(all_projects, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print(f"\n  Total projects : {len(all_projects):,}")
    print(f"  Written        → {out_path}\n")

    # Keyword summary
    kw_counts = Counter(kw for p in all_projects for kw in p["_matched_terms"])
    print("Keyword hits across all matched projects:")
    for kw, n in kw_counts.most_common(20):
        print(f"  {n:5d}  {kw}")


if __name__ == "__main__":
    main()
