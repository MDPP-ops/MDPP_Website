#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["requests", "tqdm"]
# ///
"""
fetch_erasmus_bulk.py
═════════════════════
Downloads all Erasmus+ project CSV files from the official Project Results
Platform, filters them against the textile/fashion keyword list, and writes
a JSON file compatible with the network visualisations.

WHY NOT CORDIS
  Erasmus+ is entirely separate from CORDIS (which covers Horizon/FP7 only).
  The canonical source is the Erasmus+ Project Results Platform:
    https://erasmus-plus.ec.europa.eu/projects/projects-lists
  It publishes one CSV per action type, updated weekly, with date-stamped
  filenames.  This script scrapes that page for current URLs first (so it
  never goes stale), then falls back to the known-good hardcoded URLs.

ACTION TYPES DOWNLOADED
  KA1  Learning Mobility (2014-2025, one file per year)   — includes VET
  KA2  Cooperation / Partnerships (2014-2020, 2021-2027)  — main research layer
  KA3  Policy reform (2014-2020)
  Jean Monnet, Sports                                     — skipped by default

USAGE
  uv run fetch_erasmus_bulk.py
  uv run fetch_erasmus_bulk.py --out erasmus_textile.json
  uv run fetch_erasmus_bulk.py --cache-dir ~/erasmus_cache
  uv run fetch_erasmus_bulk.py --all-actions     # include Jean Monnet & Sports
  uv run fetch_erasmus_bulk.py --ka2-only        # KA2 files only (fastest)
  uv run fetch_erasmus_bulk.py --max-age 3       # re-download after 3 days
"""

import argparse
import csv
import io
import json
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

try:
    import requests
    from tqdm import tqdm
except ImportError:
    sys.exit("Run with:  uv run fetch_erasmus_bulk.py")


# ─── Keyword list (same set used across all network tools) ───────────────────
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

# ─── Fallback hardcoded URLs (scraped 2026-04-03, updated weekly by EC) ──────
FALLBACK_URLS = [
    # KA1 — Learning Mobility of Individuals
    ("KA1 2014", "https://ec.europa.eu/programmes//erasmus-plus/project-result-content/aa12fcc2-dd48-4024-8a83-f15efb31dd80/ErasmusPlus_KA1_2014_LearningMobilityOfIndividuals_Projects_Overview_2026-04-02.csv"),
    ("KA1 2015", "https://ec.europa.eu/programmes//erasmus-plus/project-result-content/ab5748d1-3927-4210-a5aa-fcc02ddb75ea/ErasmusPlus_KA1_2015_LearningMobilityOfIndividuals_Projects_Overview_2026-04-02.csv"),
    ("KA1 2016", "https://ec.europa.eu/programmes//erasmus-plus/project-result-content/9b0da927-e046-48dd-bbfe-0eaa89730a28/ErasmusPlus_KA1_2016_LearningMobilityOfIndividuals_Projects_Overview_2026-04-02.csv"),
    ("KA1 2017", "https://ec.europa.eu/programmes//erasmus-plus/project-result-content/8fd9c6e3-8a4c-40cd-945d-d4d78e5c8d6a/ErasmusPlus_KA1_2017_LearningMobilityOfIndividuals_Projects_Overview_2026-04-02.csv"),
    ("KA1 2018", "https://ec.europa.eu/programmes//erasmus-plus/project-result-content/ba03484b-4d39-4a9a-b3b3-8e3b48a1f3b8/ErasmusPlus_KA1_2018_LearningMobilityOfIndividuals_Projects_Overview_2026-04-02.csv"),
    ("KA1 2019", "https://ec.europa.eu/programmes//erasmus-plus/project-result-content/5d5a7a1c-8b32-4d73-8a62-1b3f4b2c5d7e/ErasmusPlus_KA1_2019_LearningMobilityOfIndividuals_Projects_Overview_2026-04-02.csv"),
    ("KA1 2020", "https://ec.europa.eu/programmes//erasmus-plus/project-result-content/0ea9d3c7-767d-4101-bb15-dc22fef2024e/ErasmusPlus_KA1_2020_LearningMobilityOfIndividuals_Projects_Overview_2026-04-02.csv"),
    ("KA1 2021", "https://ec.europa.eu/programmes//erasmus-plus/project-result-content/9248e5e0-4677-46d4-afb7-55a4249ea334/ErasmusPlus_KA1_2021_LearningMobilityOfIndividuals_Projects_Overview_2026-04-02.csv"),
    ("KA1 2022", "https://ec.europa.eu/programmes//erasmus-plus/project-result-content/331b1343-f3f1-4d4e-8792-df54eb1432aa/ErasmusPlus_KA1_2022_LearningMobilityOfIndividuals_Projects_Overview_2026-04-02.csv"),
    ("KA1 2023", "https://ec.europa.eu/programmes//erasmus-plus/project-result-content/04ddbe7d-f8a4-4174-a54e-f7fab752215e/ErasmusPlus_KA1_2023_LearningMobilityOfIndividuals_Projects_Overview_2026-04-02.csv"),
    ("KA1 2024", "https://ec.europa.eu/programmes//erasmus-plus/project-result-content/3f6a9b2c-1d4e-5f8a-9b0c-2d3e4f5a6b7c/ErasmusPlus_KA1_2024_LearningMobilityOfIndividuals_Projects_Overview_2026-04-02.csv"),
    ("KA1 2025", "https://ec.europa.eu/programmes//erasmus-plus/project-result-content/aaf7e8fa-c4f1-4d9c-8e91-d71046fec68b/ErasmusPlus_KA1_2025_LearningMobilityOfIndividuals_Projects_Overview_2026-04-02.csv"),
    # KA2 — Cooperation / Partnerships (most relevant for textile research)
    ("KA2 2014-2020", "https://ec.europa.eu/programmes//erasmus-plus/project-result-content/bcba2d17-5d75-4bce-a935-3061d458a82d/ErasmusPlus_KA2_2014-2020_CooperationForInnovationAndTheExchangeOfGoodPractices_Projects_Overview_2026-04-02.csv"),
    ("KA2 2021-2027", "https://ec.europa.eu/programmes//erasmus-plus/project-result-content/119fa0ad-a537-4253-8071-4d070b8cc29b/ErasmusPlus_KA2_2021-2027_PartnershipsForCooperationAndExchangesOfPractices_Projects_Overview_2026-04-02.csv"),
    # KA3
    ("KA3 2014-2020", "https://ec.europa.eu/programmes//erasmus-plus/project-result-content/fc76e928-95e1-4a41-83f3-df7d299f0732/ErasmusPlus_KA3_2014-2020_SupportForPolicyReform_Projects_Overview_2026-04-02.csv"),
    # Jean Monnet & Sports (skipped by default)
    ("Jean Monnet", "https://ec.europa.eu/programmes//erasmus-plus/project-result-content/47ce5013-c891-44e5-90a8-db4904a5e5b4/ErasmusPlus_JeanMonnet_Projects_Overview_2026-04-02.csv"),
    ("Sports",      "https://ec.europa.eu/programmes//erasmus-plus/project-result-content/a63ffbf5-9af2-4262-ad28-87e4acd27227/ErasmusPlus_Sports_Projects_Overview_2026-04-02.csv"),
]

PROJECTS_LIST_URL = "https://erasmus-plus.ec.europa.eu/projects/projects-lists"

# Which labels to skip by default (unless --all-actions)
SKIP_BY_DEFAULT = {"Jean Monnet", "Sports"}


# ─── Helpers ─────────────────────────────────────────────────────────────────

def is_fresh(path: Path, max_age_days: int) -> bool:
    if not path.exists():
        return False
    return datetime.now() - datetime.fromtimestamp(path.stat().st_mtime) \
           < timedelta(days=max_age_days)


def scrape_current_urls() -> list[tuple[str, str]]:
    """
    Fetch the projects-lists page and extract all CSV download hrefs.
    Returns list of (label, url) tuples ordered as on the page.
    Raises on network error so caller can fall back.
    """
    print("  Fetching current CSV URLs from projects-lists page…")
    resp = requests.get(PROJECTS_LIST_URL, timeout=30,
                        headers={"User-Agent": "Mozilla/5.0 (research-tool)"})
    resp.raise_for_status()
    html = resp.text

    results = []
    # Match anchor tags with href ending in .csv
    for m in re.finditer(
        r'<a\b[^>]*href=["\']([^"\']+\.csv)["\'][^>]*>(.*?)</a>',
        html, re.IGNORECASE | re.DOTALL
    ):
        href = m.group(1)
        label_raw = re.sub(r'<[^>]+>', '', m.group(2)).strip()
        # Shorten label to action type for display
        label = _shorten_label(label_raw)
        if href.startswith('http'):
            results.append((label, href))
        else:
            results.append((label, "https://erasmus-plus.ec.europa.eu" + href))

    print(f"  Found {len(results)} CSV files on page")
    return results


def _shorten_label(full: str) -> str:
    """Extract a short key like 'KA2 2014-2020' from the full title."""
    full = full.replace('\u00a0', ' ').strip()
    m = re.search(r'(KA\d+)[^(]*\(([^)]+)\)', full)
    if m:
        return f"{m.group(1)} {m.group(2)}"
    if 'Jean Monnet' in full:
        return 'Jean Monnet'
    if 'Sports' in full:
        return 'Sports'
    return full[:40]


def download_csv(label: str, url: str, cache_path: Path, max_age: int) -> bytes | None:
    """Download url → cache_path. Returns bytes or None on failure."""
    if is_fresh(cache_path, max_age):
        size_kb = cache_path.stat().st_size // 1024
        print(f"  [cache]  {label:30s}  ({size_kb} KB)")
        return cache_path.read_bytes()

    try:
        resp = requests.get(url, timeout=60,
                            headers={"User-Agent": "Mozilla/5.0 (research-tool)"})
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  [FAIL]   {label:30s}  {e}")
        return None

    data = resp.content
    cache_path.write_bytes(data)
    print(f"  [dl]     {label:30s}  ({len(data)//1024} KB)")
    return data


def parse_csv(raw: bytes) -> tuple[list[str], list[dict]]:
    """Parse Erasmus+ CSV (comma or semicolon delimited, UTF-8 BOM). Returns (headers, rows)."""
    text = raw.decode("utf-8-sig", errors="replace")
    # Detect delimiter from first line
    first_line = text.split('\n', 1)[0]
    delimiter = ';' if first_line.count(';') > first_line.count(',') else ','
    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    rows = list(reader)
    headers = reader.fieldnames or []
    return list(headers), rows


def match_keywords(text: str) -> list[str]:
    t = (text or "").lower()
    return [kw for kw in KEYWORDS if kw in t]


def safe(val) -> str:
    return str(val).strip() if val is not None else ""


def find_col(row: dict, *candidates: str) -> str:
    """Return value of first matching column (case-insensitive)."""
    lower = {k.lower(): v for k, v in row.items()}
    for c in candidates:
        v = lower.get(c.lower())
        if v is not None:
            return safe(v)
    return ""


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out",         default="erasmus_textile_projects.json")
    ap.add_argument("--cache-dir",   default="./erasmus_cache")
    ap.add_argument("--all-actions", action="store_true",
                    help="Include Jean Monnet and Sports files")
    ap.add_argument("--ka2-only",    action="store_true",
                    help="Download KA2 files only (fastest, most relevant)")
    ap.add_argument("--max-age",     type=int, default=7)
    args = ap.parse_args()

    cache_dir = Path(args.cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)

    # ── Step 1: get current download URLs ────────────────────────────────────
    print(f"\n{'═'*62}")
    print("  Erasmus+ Project Results Platform — bulk fetch")
    print(f"{'═'*62}")

    try:
        file_list = scrape_current_urls()
    except Exception as e:
        print(f"  WARNING: could not scrape page ({e}), using fallback URLs")
        file_list = FALLBACK_URLS

    # Apply action filters
    if args.ka2_only:
        file_list = [(l, u) for l, u in file_list if l.startswith("KA2")]
    elif not args.all_actions:
        file_list = [(l, u) for l, u in file_list if l not in SKIP_BY_DEFAULT]

    print(f"\n  {len(file_list)} action files selected\n")

    # ── Step 2: download CSVs ─────────────────────────────────────────────────
    matched_projects: list[dict] = []
    seen_ids: set[str] = set()
    headers_printed = False

    for label, url in tqdm(file_list, desc="  Downloading", ncols=70, leave=False):
        safe_label = re.sub(r'[^\w\-]', '_', label)
        cache_path = cache_dir / f"erasmus_{safe_label}.csv"

        raw = download_csv(label, url, cache_path, args.max_age)
        if raw is None:
            continue

        headers, rows = parse_csv(raw)

        if not headers_printed:
            print(f"\n  CSV columns detected: {headers[:10]}")
            headers_printed = True

        count = 0
        for row in rows:
            # Build haystack from title + summary/description
            title   = find_col(row, "Project Title", "Title", "ProjectTitle", "name")
            summary = find_col(row, "Project Summary", "Summary", "Description",
                               "Project Summary/Abstract", "Objective", "Abstract")
            org     = find_col(row, "Coordinating Organisation", "Coordinator",
                               "Organisation", "CoordinatingOrganisation")
            country = find_col(row, "Coordinator Country", "Country",
                               "CoordinatorCountry", "Country Code")
            pid     = find_col(row, "Project Identifier", "Reference",
                               "Project Reference", "ProjectIdentifier", "ID")
            start   = find_col(row, "Start Date", "StartDate", "Project Start Date")
            end     = find_col(row, "End Date",   "EndDate",   "Project End Date")
            call    = find_col(row, "Call Year", "Year", "Round", "CallYear")

            hits = match_keywords(title + " " + summary)
            if not hits:
                continue

            # Deduplicate by project ID
            if pid and pid in seen_ids:
                continue
            if pid:
                seen_ids.add(pid)

            # Build partner list from numbered partner columns
            partners = []
            for k, v in row.items():
                if re.match(r'partner\s*(name|organisation)?\s*\d+', k, re.I) and safe(v):
                    partners.append({"organisation": safe(v), "role": "Partner"})
            if org:
                partners.insert(0, {"organisation": org,
                                    "country": country, "role": "Coordinator"})

            matched_projects.append({
                "project_id":       pid,
                "title_en":         title,
                "funding_scheme":   label,
                "programme":        "Erasmus+",
                "start_date":       start,
                "end_date":         end,
                "call_year":        call,
                "status":           find_col(row, "Status", "Project Status"),
                "objective":        summary[:1200],
                "_pi_institution":  org,
                "_pi_country":      country,
                "_pi_name":         find_col(row, "Contact Name", "Contact",
                                             "Coordinator Contact"),
                "_matched_terms":   hits,
                "project_members":  partners,
                "project_page_url": (
                    f"https://erasmus-plus.ec.europa.eu/projects/search/project/{pid}"
                    if pid else ""
                ),
            })
            count += 1

        if count:
            print(f"  ✓ {label:30s}  {count} textile matches  ({len(rows):,} total rows)")

    # ── Step 3: write output ──────────────────────────────────────────────────
    print(f"\n{'─'*62}")
    print(f"  Total matched : {len(matched_projects):,}")

    out_path = Path(args.out)
    out_path.write_text(
        json.dumps(matched_projects, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"  Written → {out_path}\n")

    # Keyword summary
    from collections import Counter
    kw_counts = Counter(kw for p in matched_projects for kw in p["_matched_terms"])
    print("Matched keywords:")
    for kw, n in kw_counts.most_common(15):
        print(f"  {n:5d}  {kw}")

    print(f"\nDone.  Load {out_path} into nwo_textile_raw_network.html to visualise.")


if __name__ == "__main__":
    main()
