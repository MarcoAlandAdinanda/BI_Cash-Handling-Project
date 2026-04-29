"""
Postal Code Scraper — Kalimantan Selatan (South Kalimantan)
===========================================================
Scrapes all postal code data from postcode.id for the province of
Kalimantan Selatan, traversing three administrative levels:

    Provinsi → Kabupaten/Kota → Kecamatan → Kelurahan/Desa

Output: JSON file with full administrative hierarchy.

Usage:
    pip install requests beautifulsoup4
    python webscraping/scrape_postcode_kalsel.py
"""

import json
import time
import random
import logging
import re
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ─── Configuration ───────────────────────────────────────────────────────────

BASE_URL = "https://postcode.id"
PROVINCE_URL = f"{BASE_URL}/kodepos/provinsi/kalimantan-selatan/"

# Output path (relative to this script's location → project root)
OUTPUT_JSON = Path(__file__).resolve().parent.parent / "kalsel_kodepos.json"

# Polite delay between requests (seconds) to avoid overwhelming the server
DELAY_RANGE = (1.0, 2.5)

# HTTP headers to mimic a real browser
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "id-ID,id;q=0.9,en;q=0.5",
}

MAX_RETRIES = 3

# ─── Logging ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-7s │ %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("postcode_scraper")

# ─── HTTP Session ────────────────────────────────────────────────────────────

session = requests.Session()
session.headers.update(HEADERS)


def polite_delay():
    """Sleep a random duration to be polite to the server."""
    time.sleep(random.uniform(*DELAY_RANGE))


def fetch_page(url: str) -> BeautifulSoup | None:
    """Fetch a URL and return a BeautifulSoup object. Retries on failure."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=30)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "html.parser")
        except requests.RequestException as e:
            log.warning(f"  ⚠ Attempt {attempt}/{MAX_RETRIES} failed for {url}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(2 * attempt)  # exponential back-off
    log.error(f"  ✖ Failed to fetch {url} after {MAX_RETRIES} attempts.")
    return None


# ─── Parsing Functions ───────────────────────────────────────────────────────


def parse_kabupaten_list(soup: BeautifulSoup) -> list[dict]:
    """
    Parse the province page to extract kabupaten/kota names and URLs.
    Links are in the format: /kodepos/kota/<slug>/
    """
    kabupaten_list = []
    # Find all links to kabupaten/kota pages
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if "/kodepos/kota/" in href and href.count("/") >= 4:
            name = a_tag.get_text(strip=True)
            if name and name not in [k["nama"] for k in kabupaten_list]:
                full_url = href if href.startswith("http") else BASE_URL + href
                kabupaten_list.append({"nama": name, "url": full_url})
    return kabupaten_list


def parse_kecamatan_list(soup: BeautifulSoup) -> list[dict]:
    """
    Parse a kabupaten page to extract kecamatan names and URLs.
    Links are in the format: /kodepos/kecamatan/<slug>/
    """
    kecamatan_list = []
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if "/kodepos/kecamatan/" in href and href.count("/") >= 4:
            name = a_tag.get_text(strip=True)
            if name and name not in [k["nama"] for k in kecamatan_list]:
                full_url = href if href.startswith("http") else BASE_URL + href
                kecamatan_list.append({"nama": name, "url": full_url})
    return kecamatan_list


def parse_kelurahan_list(soup: BeautifulSoup) -> list[dict]:
    """
    Parse a kecamatan page to extract kelurahan/desa names and their kode pos.

    Uses the structured table (class='pc-dist-table') which has rows with
    class='pc-village-row'. Each row has:
      - td[data-label="Nama Kelurahan/Desa"] → kelurahan name (inside an <a> tag)
      - td[data-label="Kodepos"] → postal code as plain text

    Filters entries to only include those belonging to Kalimantan Selatan,
    since some kecamatan names (e.g. "Tebing Tinggi") are shared across
    multiple provinces and the page may list entries from all of them.
    """
    kelurahan_list = []
    province_filter = "kalimantan-selatan"

    # Primary strategy: Parse the structured data table
    table = soup.find("table", class_="pc-dist-table")
    if table:
        for row in table.find_all("tr", class_="pc-village-row"):
            name_td = row.find("td", attrs={"data-label": "Nama Kelurahan/Desa"})
            kodepos_td = row.find("td", attrs={"data-label": "Kodepos"})

            if name_td:
                a_tag = name_td.find("a")

                # Filter: only include entries whose link URL belongs to our province
                if a_tag and a_tag.get("href"):
                    href = a_tag["href"]
                    if province_filter not in href:
                        continue

                name = a_tag.get_text(strip=True) if a_tag else name_td.get_text(strip=True)
                kode_pos = kodepos_td.get_text(strip=True) if kodepos_td else None

                if name:
                    kelurahan_list.append({
                        "nama": name,
                        "kode_pos": kode_pos,
                    })
        return kelurahan_list

    # Fallback: Parse the "Akses Cepat" chip links (class='pc-chip')
    # Structure: <a class="pc-chip">Name<small>XXXXX</small></a>
    for chip in soup.find_all("a", class_="pc-chip"):
        # Filter: only include entries whose link URL belongs to our province
        href = chip.get("href", "")
        if href and province_filter not in href:
            continue

        small_tag = chip.find("small")
        if small_tag:
            kode_pos = small_tag.get_text(strip=True)
            # Remove the <small> element to get only the name text
            small_tag.extract()
            name = chip.get_text(strip=True)
        else:
            name = chip.get_text(strip=True)
            kode_pos = None

        if name:
            kelurahan_list.append({
                "nama": name,
                "kode_pos": kode_pos,
            })

    return kelurahan_list


# ─── Main Scraping Pipeline ─────────────────────────────────────────────────


def scrape_kecamatan(kec_name: str, kec_url: str) -> dict:
    """Scrape a single kecamatan page and return its data with kelurahan list."""
    log.info(f"      📍 Kecamatan: {kec_name}")
    polite_delay()
    soup = fetch_page(kec_url)
    if soup is None:
        return {"nama": kec_name, "kelurahan": []}

    kelurahan_list = parse_kelurahan_list(soup)
    log.info(f"         → {len(kelurahan_list)} kelurahan/desa found")

    return {
        "nama": kec_name,
        "kelurahan": kelurahan_list,
    }


def scrape_kabupaten(kab_name: str, kab_url: str) -> dict:
    """Scrape a single kabupaten page and all its kecamatan."""
    log.info(f"   🏙️  Kabupaten/Kota: {kab_name}")
    polite_delay()
    soup = fetch_page(kab_url)
    if soup is None:
        return {"nama": kab_name, "kecamatan": []}

    kecamatan_entries = parse_kecamatan_list(soup)
    log.info(f"      → {len(kecamatan_entries)} kecamatan found")

    kecamatan_data = []
    for idx, kec in enumerate(kecamatan_entries, 1):
        log.info(f"      [{idx}/{len(kecamatan_entries)}]")
        kec_data = scrape_kecamatan(kec["nama"], kec["url"])
        kecamatan_data.append(kec_data)

    return {
        "nama": kab_name,
        "kecamatan": kecamatan_data,
    }


def scrape_province() -> dict:
    """Scrape the entire province of Kalimantan Selatan."""
    log.info("═" * 60)
    log.info("🗺️  Scraping Kode Pos — Kalimantan Selatan")
    log.info("═" * 60)

    soup = fetch_page(PROVINCE_URL)
    if soup is None:
        log.error("Failed to fetch the province page. Aborting.")
        return {}

    kabupaten_entries = parse_kabupaten_list(soup)
    log.info(f"Found {len(kabupaten_entries)} kabupaten/kota")
    log.info("─" * 60)

    kabupaten_data = []
    for idx, kab in enumerate(kabupaten_entries, 1):
        log.info(f"\n[{idx}/{len(kabupaten_entries)}]")
        kab_data = scrape_kabupaten(kab["nama"], kab["url"])
        kabupaten_data.append(kab_data)

    # Build the final structured output
    result = {
        "provinsi": "Kalimantan Selatan",
        "sumber": "https://postcode.id",
        "jumlah_kabupaten_kota": len(kabupaten_data),
        "jumlah_kecamatan": sum(len(k["kecamatan"]) for k in kabupaten_data),
        "jumlah_kelurahan_desa": sum(
            len(kel)
            for k in kabupaten_data
            for kec in k["kecamatan"]
            for kel in [kec["kelurahan"]]
        ),
        "kabupaten_kota": kabupaten_data,
    }

    return result


# ─── Entry Point ─────────────────────────────────────────────────────────────


def main():
    result = scrape_province()

    if not result:
        log.error("No data collected. Exiting.")
        return

    # Save to JSON
    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    log.info("\n" + "═" * 60)
    log.info(f"✅ Data saved to: {OUTPUT_JSON}")
    log.info(f"   Kabupaten/Kota : {result['jumlah_kabupaten_kota']}")
    log.info(f"   Kecamatan      : {result['jumlah_kecamatan']}")
    log.info(f"   Kelurahan/Desa : {result['jumlah_kelurahan_desa']}")
    log.info("═" * 60)

    # Print a small sample for verification
    sample_kab = result["kabupaten_kota"][0]
    sample_kec = sample_kab["kecamatan"][0] if sample_kab["kecamatan"] else None
    log.info("\n📋 Sample output:")
    log.info(f"   Kabupaten: {sample_kab['nama']}")
    if sample_kec:
        log.info(f"   Kecamatan: {sample_kec['nama']}")
        if sample_kec["kelurahan"]:
            log.info(f"   Kelurahan: {sample_kec['kelurahan'][0]}")


if __name__ == "__main__":
    main()
