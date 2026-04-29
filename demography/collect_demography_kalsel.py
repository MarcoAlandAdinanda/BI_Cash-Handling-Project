"""
Collect Demography Data for Kalimantan Selatan (South Kalimantan)
=================================================================

This script fetches administrative division data for Kalimantan Selatan province
from the Kemendagri (Ministry of Home Affairs) government data, served via the
emsifa/api-wilayah-indonesia open API (GitHub Pages).

Data Source:
    - Kemendagri (Kementerian Dalam Negeri / Ministry of Home Affairs)
    - Permendagri Kode dan Data Wilayah Administrasi Pemerintahan
    - API: https://github.com/emsifa/api-wilayah-indonesia

Output:
    - CSV file: kalsel_demography.csv
    - Columns: province_code, province_name, regency_code, regency_name,
               regency_type, district_code, district_name, village_code,
               village_name, village_type

Administrative Hierarchy:
    Provinsi → Kabupaten/Kota → Kecamatan → Desa/Kelurahan

Usage:
    python collect_demography_kalsel.py
"""

import csv
import json
import os
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime


# ==============================================================================
# Configuration
# ==============================================================================

# Kalimantan Selatan province ID in Kemendagri coding system
PROVINCE_ID = "63"
PROVINCE_NAME = "KALIMANTAN SELATAN"

# API base URL (emsifa/api-wilayah-indonesia - based on Kemendagri data)
API_BASE = "https://emsifa.github.io/api-wilayah-indonesia/api"

# Output file
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "kalsel_demography.csv")

# Request settings
REQUEST_DELAY = 0.3  # seconds between API calls to be respectful
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds between retries


# ==============================================================================
# Helper Functions
# ==============================================================================

def fetch_json(url: str) -> list:
    """
    Fetch JSON data from the API with retry logic.

    Args:
        url: The API endpoint URL.

    Returns:
        Parsed JSON data as a list of dictionaries.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "BI-Cash-Handling-Demography-Collector/1.0"}
            )
            with urllib.request.urlopen(req, timeout=30) as response:
                data = json.loads(response.read().decode("utf-8"))
                return data
        except urllib.error.HTTPError as e:
            print(f"  [!] HTTP Error {e.code} for {url} (attempt {attempt}/{MAX_RETRIES})")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
        except urllib.error.URLError as e:
            print(f"  [!] URL Error: {e.reason} for {url} (attempt {attempt}/{MAX_RETRIES})")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
        except Exception as e:
            print(f"  [!] Unexpected error: {e} (attempt {attempt}/{MAX_RETRIES})")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)

    print(f"  [X] Failed to fetch {url} after {MAX_RETRIES} attempts. Skipping.")
    return []


def classify_regency(regency_name: str, regency_id: str) -> str:
    """
    Classify a regency as 'Kabupaten' or 'Kota' based on its name and code.

    Per Kemendagri coding:
        - Kabupaten: regency code digits 3-4 are 01-69
        - Kota: regency code digits 3-4 are 71-99

    Args:
        regency_name: The regency name (e.g., "KABUPATEN BANJAR" or "KOTA BANJARMASIN").
        regency_id: The regency code (e.g., "6303" or "6371").

    Returns:
        "Kabupaten" or "Kota".
    """
    if regency_name.upper().startswith("KOTA "):
        return "Kota"
    elif regency_name.upper().startswith("KABUPATEN "):
        return "Kabupaten"
    else:
        # Fallback: use the code convention
        suffix = int(regency_id[2:4])
        return "Kota" if suffix >= 71 else "Kabupaten"


def classify_village(village_id: str, regency_type: str) -> str:
    """
    Classify a village as 'Kelurahan' or 'Desa' based on Kemendagri code convention.

    Per Kemendagri coding, the 7th digit (first digit of the 4-digit village code):
        - 1xxx = Kelurahan (urban village/ward)
        - 2xxx = Desa (rural village)

    As a secondary heuristic:
        - Kota (cities) typically contain Kelurahan
        - Kabupaten (regencies) typically contain Desa (but can also have Kelurahan)

    Args:
        village_id: The 10-digit village code (e.g., "6301010001").
        regency_type: The parent regency type ("Kabupaten" or "Kota").

    Returns:
        "Kelurahan" or "Desa".
    """
    if len(village_id) >= 10:
        # The 7th digit (index 6) determines the type
        village_type_digit = village_id[6]
        if village_type_digit == "1":
            return "Kelurahan"
        elif village_type_digit == "2":
            return "Desa"

    # Fallback heuristic based on parent regency type
    return "Kelurahan" if regency_type == "Kota" else "Desa"


def clean_name(name: str) -> str:
    """
    Clean and format an administrative name to title case.

    Args:
        name: Raw name string (typically UPPERCASE from the API).

    Returns:
        Title-cased, stripped name.
    """
    return name.strip().title()


def strip_prefix(name: str) -> str:
    """
    Remove 'KABUPATEN ' or 'KOTA ' prefix from a regency name.

    Args:
        name: Full regency name (e.g., "KABUPATEN BANJAR").

    Returns:
        Name without prefix (e.g., "BANJAR").
    """
    upper = name.upper().strip()
    if upper.startswith("KABUPATEN "):
        return name.strip()[len("KABUPATEN "):]
    elif upper.startswith("KOTA "):
        return name.strip()[len("KOTA "):]
    return name.strip()


# ==============================================================================
# Main Collection Logic
# ==============================================================================

def collect_demography():
    """
    Main function to collect demography data for Kalimantan Selatan.

    Fetches data in hierarchical order:
        1. Kabupaten/Kota (Regencies/Cities) in the province
        2. Kecamatan (Districts) in each regency
        3. Desa/Kelurahan (Villages) in each district

    Writes all records to a CSV file.
    """
    print("=" * 70)
    print("  DEMOGRAPHY DATA COLLECTOR - KALIMANTAN SELATAN")
    print("  Source: Kemendagri (via emsifa/api-wilayah-indonesia)")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    print()

    all_records = []

    # ---- Step 1: Fetch Regencies (Kabupaten/Kota) ----
    print("[1/3] Fetching Kabupaten/Kota in Kalimantan Selatan...")
    regencies_url = f"{API_BASE}/regencies/{PROVINCE_ID}.json"
    regencies = fetch_json(regencies_url)
    print(f"  Found {len(regencies)} Kabupaten/Kota")
    print()

    if not regencies:
        print("[ERROR] No regencies found. Please check your internet connection.")
        sys.exit(1)

    # ---- Step 2 & 3: Fetch Districts and Villages for each Regency ----
    total_districts = 0
    total_villages = 0

    for i, regency in enumerate(regencies, 1):
        regency_id = regency["id"]
        regency_name_raw = regency["name"]
        regency_type = classify_regency(regency_name_raw, regency_id)
        regency_name_clean = clean_name(strip_prefix(regency_name_raw))

        print(f"[2/3] ({i}/{len(regencies)}) Processing {regency_type} {regency_name_clean}...")

        # Fetch districts (kecamatan)
        districts_url = f"{API_BASE}/districts/{regency_id}.json"
        time.sleep(REQUEST_DELAY)
        districts = fetch_json(districts_url)
        total_districts += len(districts)
        print(f"  -> {len(districts)} Kecamatan found")

        for j, district in enumerate(districts, 1):
            district_id = district["id"]
            district_name_clean = clean_name(district["name"])

            # Fetch villages (desa/kelurahan)
            villages_url = f"{API_BASE}/villages/{district_id}.json"
            time.sleep(REQUEST_DELAY)
            villages = fetch_json(villages_url)
            total_villages += len(villages)

            for village in villages:
                village_id = village["id"]
                village_name_clean = clean_name(village["name"])
                village_type = classify_village(village_id, regency_type)

                record = {
                    "province_code": PROVINCE_ID,
                    "province_name": clean_name(PROVINCE_NAME),
                    "regency_code": regency_id,
                    "regency_name": regency_name_clean,
                    "regency_type": regency_type,
                    "district_code": district_id,
                    "district_name": district_name_clean,
                    "village_code": village_id,
                    "village_name": village_name_clean,
                    "village_type": village_type,
                }
                all_records.append(record)

            # Progress indicator for large districts
            if j % 5 == 0 or j == len(districts):
                print(f"     Kecamatan processed: {j}/{len(districts)} "
                      f"(villages so far: {total_villages})")

    print()

    # ---- Step 4: Write to CSV ----
    print(f"[3/3] Writing {len(all_records)} records to CSV...")

    fieldnames = [
        "province_code",
        "province_name",
        "regency_code",
        "regency_name",
        "regency_type",
        "district_code",
        "district_name",
        "village_code",
        "village_name",
        "village_type",
    ]

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_records)

    print(f"  -> Saved to: {OUTPUT_FILE}")
    print()

    # ---- Summary ----
    print("=" * 70)
    print("  COLLECTION SUMMARY")
    print("=" * 70)
    print(f"  Province       : {clean_name(PROVINCE_NAME)} (Code: {PROVINCE_ID})")
    print(f"  Kabupaten/Kota : {len(regencies)}")
    print(f"  Kecamatan      : {total_districts}")
    print(f"  Desa/Kelurahan : {total_villages}")
    print(f"    - Kelurahan  : {sum(1 for r in all_records if r['village_type'] == 'Kelurahan')}")
    print(f"    - Desa       : {sum(1 for r in all_records if r['village_type'] == 'Desa')}")
    print(f"  Total Records  : {len(all_records)}")
    print(f"  Output File    : {OUTPUT_FILE}")
    print(f"  Completed      : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)


if __name__ == "__main__":
    collect_demography()
