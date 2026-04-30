"""
BPS "Kecamatan Dalam Angka" Report Scraper
==========================================
Automatically scrapes publication reports from BPS (Badan Pusat Statistik)
websites for kecamatan in Kalimantan Selatan.

For each kecamatan, the script:
1. Navigates to the BPS kabupaten publication page
2. Searches for "Kecamatan {name} Dalam Angka 2025"
3. Falls back to the latest available year if 2025 isn't found
4. Extracts the PDF download link
5. Saves all links to report_links.csv
6. Downloads PDFs to ../kumpulan_pdf/

Usage:
    pip install -r requirements.txt
    playwright install chromium
    python data_penduduk/scrape_bps_reports.py
"""

import asyncio
import csv
import logging
import os
import random
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
from playwright_stealth import Stealth

# ─── Configuration ───────────────────────────────────────────────────────────

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
CSV_OUTPUT = SCRIPT_DIR / "report_links.csv"
PDF_DIR = PROJECT_DIR / "kumpulan_pdf"

# Anti-bot delays (seconds)
DELAY_BETWEEN_PAGES = (3.0, 6.0)
DELAY_SHORT = (1.0, 2.5)

VIEWPORT_WIDTH = 1366
VIEWPORT_HEIGHT = 768

# Years to try in order
YEARS_TO_TRY = [2025, 2024, 2023]

# ─── Logging ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-7s │ %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bps_scraper")

# ─── BPS Subdomain Mapping ──────────────────────────────────────────────────

BPS_SUBDOMAINS = {
    "tanah laut": "tanahlautkab",
    "kota baru": "kotabarukab",
    "banjar": "banjarkab",
    "barito kuala": "baritokualakab",
    "tapin": "tapinkab",
    "hulu sungai selatan": "hulusungaiselatankab",
    "hulu sungai tengah": "hulusungaitengahkab",
    "hulu sungai utara": "hulusungaiutarakab",
    "tabalong": "tabalongkab",
    "tanah bumbu": "tanahbumbukab",
    "balangan": "balangankab",
    "kota banjarmasin": "banjarmasinkota",
    "kota banjar baru": "banjarbarukota",
}

# Fallback subdomains to try if primary fails
BPS_SUBDOMAIN_FALLBACKS = {
    "kota baru": ["kotabaru"],
    "barito kuala": ["baaborinkab"],
    "kota banjar baru": ["baborinkota"],
}

# ─── Kecamatan Data (Standardized Names) ────────────────────────────────────

KECAMATAN_LIST = [
    # Kabupaten Tanah Laut (11)
    ("Takisung", "tanah laut"),
    ("Jorong", "tanah laut"),
    ("Pelaihari", "tanah laut"),
    ("Kurau", "tanah laut"),
    ("Bati-Bati", "tanah laut"),
    ("Panyipatan", "tanah laut"),
    ("Kintap", "tanah laut"),
    ("Tambang Ulang", "tanah laut"),
    ("Batu Ampar", "tanah laut"),
    ("Bajuin", "tanah laut"),
    ("Bumi Makmur", "tanah laut"),
    # Kabupaten Kota Baru (21)
    ("Pulau Sembilan", "kota baru"),
    ("Pulau Laut Barat", "kota baru"),
    ("Pulau Laut Selatan", "kota baru"),
    ("Pulau Laut Timur", "kota baru"),
    ("Pulau Sebuku", "kota baru"),
    ("Pulau Laut Utara", "kota baru"),
    ("Kelumpang Selatan", "kota baru"),
    ("Kelumpang Hulu", "kota baru"),
    ("Kelumpang Tengah", "kota baru"),
    ("Kelumpang Utara", "kota baru"),
    ("Pamukan Selatan", "kota baru"),
    ("Sampanahan", "kota baru"),
    ("Pamukan Utara", "kota baru"),
    ("Hampang", "kota baru"),
    ("Sungai Durian", "kota baru"),
    ("Kelumpang Barat", "kota baru"),
    ("Pulau Laut Tengah", "kota baru"),
    ("Pulau Laut Kepulauan", "kota baru"),
    ("Kelumpang Hilir", "kota baru"),
    ("Pamukan Barat", "kota baru"),
    ("Pulau Laut Tanjung Selayar", "kota baru"),
    # Kabupaten Banjar (20)
    ("Aluh-Aluh", "banjar"),
    ("Kertak Hanyar", "banjar"),
    ("Gambut", "banjar"),
    ("Sungai Tabuk", "banjar"),
    ("Martapura", "banjar"),
    ("Karang Intan", "banjar"),
    ("Astambul", "banjar"),
    ("Simpang Empat", "banjar"),
    ("Pengaron", "banjar"),
    ("Sungai Pinang", "banjar"),
    ("Aranio", "banjar"),
    ("Mataraman", "banjar"),
    ("Beruntung Baru", "banjar"),
    ("Martapura Barat", "banjar"),
    ("Martapura Timur", "banjar"),
    ("Sambung Makmur", "banjar"),
    ("Paramasan", "banjar"),
    ("Telaga Bauntung", "banjar"),
    ("Tatah Makmur", "banjar"),
    ("Cintapuri Darussalam", "banjar"),
    # Kabupaten Barito Kuala (17)
    ("Tabunganen", "barito kuala"),
    ("Tamban", "barito kuala"),
    ("Anjir Pasar", "barito kuala"),
    ("Anjir Muara", "barito kuala"),
    ("Alalak", "barito kuala"),
    ("Mandastana", "barito kuala"),
    ("Rantau Badauh", "barito kuala"),
    ("Belawang", "barito kuala"),
    ("Cerbon", "barito kuala"),
    ("Bakumpai", "barito kuala"),
    ("Kuripan", "barito kuala"),
    ("Tabukan", "barito kuala"),
    ("Mekar Sari", "barito kuala"),
    ("Barambai", "barito kuala"),
    ("Marabahan", "barito kuala"),
    ("Wanaraya", "barito kuala"),
    ("Jejangkit", "barito kuala"),
    # Kabupaten Tapin (12)
    ("Binuang", "tapin"),
    ("Tapin Selatan", "tapin"),
    ("Tapin Tengah", "tapin"),
    ("Tapin Utara", "tapin"),
    ("Candi Laras Selatan", "tapin"),
    ("Candi Laras Utara", "tapin"),
    ("Bakarangan", "tapin"),
    ("Piani", "tapin"),
    ("Bungur", "tapin"),
    ("Lokpaikat", "tapin"),
    ("Hatungun", "tapin"),
    ("Salam Babaris", "tapin"),
    # Kabupaten Hulu Sungai Selatan (11)
    ("Sungai Raya", "hulu sungai selatan"),
    ("Padang Batung", "hulu sungai selatan"),
    ("Telaga Langsat", "hulu sungai selatan"),
    ("Angkinang", "hulu sungai selatan"),
    ("Kandangan", "hulu sungai selatan"),
    ("Simpur", "hulu sungai selatan"),
    ("Daha Selatan", "hulu sungai selatan"),
    ("Daha Utara", "hulu sungai selatan"),
    ("Kalumpang", "hulu sungai selatan"),
    ("Loksado", "hulu sungai selatan"),
    ("Daha Barat", "hulu sungai selatan"),
    # Kabupaten Hulu Sungai Tengah (10)
    ("Haruyan", "hulu sungai tengah"),
    ("Batu Benawa", "hulu sungai tengah"),
    ("Labuan Amas Selatan", "hulu sungai tengah"),
    ("Labuan Amas Utara", "hulu sungai tengah"),
    ("Pandawan", "hulu sungai tengah"),
    ("Barabai", "hulu sungai tengah"),
    ("Batang Alai Selatan", "hulu sungai tengah"),
    ("Batang Alai Utara", "hulu sungai tengah"),
    ("Batang Alai Timur", "hulu sungai tengah"),
    ("Limpasu", "hulu sungai tengah"),
    # Kabupaten Hulu Sungai Utara (10)
    ("Danau Panggang", "hulu sungai utara"),
    ("Babirik", "hulu sungai utara"),
    ("Sungai Pandan", "hulu sungai utara"),
    ("Amuntai Selatan", "hulu sungai utara"),
    ("Amuntai Tengah", "hulu sungai utara"),
    ("Amuntai Utara", "hulu sungai utara"),
    ("Banjang", "hulu sungai utara"),
    ("Haur Gading", "hulu sungai utara"),
    ("Paminggir", "hulu sungai utara"),
    ("Sungai Tabukan", "hulu sungai utara"),
    # Kabupaten Tabalong (11)
    ("Banua Lawas", "tabalong"),
    ("Kelua", "tabalong"),
    ("Tanta", "tabalong"),
    ("Tanjung", "tabalong"),
    ("Haruai", "tabalong"),
    ("Murung Pudak", "tabalong"),
    ("Muara Uya", "tabalong"),
    ("Muara Harus", "tabalong"),
    ("Upau", "tabalong"),
    ("Jaro", "tabalong"),
    ("Bintang Ara", "tabalong"),
    # Kabupaten Tanah Bumbu (10)
    ("Batu Licin", "tanah bumbu"),
    ("Kusan Hilir", "tanah bumbu"),
    ("Sungai Loban", "tanah bumbu"),
    ("Satui", "tanah bumbu"),
    ("Kusan Hulu", "tanah bumbu"),
    ("Angsana", "tanah bumbu"),
    ("Kuranji", "tanah bumbu"),
    ("Karang Bintang", "tanah bumbu"),
    ("Simpang Empat", "tanah bumbu"),
    ("Mantewe", "tanah bumbu"),
    # Kabupaten Balangan (8)
    ("Juai", "balangan"),
    ("Halong", "balangan"),
    ("Awayan", "balangan"),
    ("Batumandi", "balangan"),
    ("Lampihong", "balangan"),
    ("Paringin", "balangan"),
    ("Paringin Selatan", "balangan"),
    ("Tebing Tinggi", "balangan"),
    # Kota Banjarmasin (5)
    ("Banjarmasin Selatan", "kota banjarmasin"),
    ("Banjarmasin Timur", "kota banjarmasin"),
    ("Banjarmasin Barat", "kota banjarmasin"),
    ("Banjarmasin Utara", "kota banjarmasin"),
    ("Banjarmasin Tengah", "kota banjarmasin"),
    # Kota Banjarbaru (5)
    ("Landasan Ulin", "kota banjar baru"),
    ("Cempaka", "kota banjar baru"),
    ("Banjarbaru Utara", "kota banjar baru"),
    ("Banjarbaru Selatan", "kota banjar baru"),
    ("Liang Anggang", "kota banjar baru"),
]

# ─── Helper Functions ────────────────────────────────────────────────────────


def human_delay(delay_range: tuple[float, float]):
    """Sleep for a randomized human-like duration."""
    return asyncio.sleep(random.uniform(*delay_range))


def get_bps_url(kabupaten: str, keyword: str) -> str:
    """Build BPS publication search URL."""
    subdomain = BPS_SUBDOMAINS[kabupaten]
    encoded_kw = quote_plus(keyword)
    return f"https://{subdomain}.bps.go.id/id/publication?keyword={encoded_kw}"


def sanitize_filename(name: str) -> str:
    """Create a safe filename from a kecamatan name."""
    return re.sub(r'[^\w\s-]', '', name).strip().replace(' ', '_')


def load_existing_csv() -> dict:
    """Load existing CSV to support resume. Returns dict keyed by kecamatan."""
    existing = {}
    if CSV_OUTPUT.exists():
        with open(CSV_OUTPUT, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = row.get("kecamatan", "")
                if key and row.get("status") == "success":
                    existing[key] = row
    return existing


def save_csv(records: list[dict]):
    """Save all records to CSV."""
    fieldnames = [
        "kecamatan", "kabupaten", "search_keyword", "year_found",
        "publication_title", "publication_url", "pdf_download_url",
        "pdf_filename", "status", "error_message",
    ]
    with open(CSV_OUTPUT, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
    log.info(f"  📄 CSV saved: {CSV_OUTPUT}")


# ─── Core Scraping ───────────────────────────────────────────────────────────


async def dismiss_popup(page):
    """Dismiss any modal popup that BPS sites sometimes show."""
    try:
        close_selectors = [
            'button:has-text("Tutup")',
            'button:has-text("Close")',
            'button.close',
            '.modal .close',
            '[data-dismiss="modal"]',
            'button[aria-label="Close"]',
        ]
        for selector in close_selectors:
            btn = page.locator(selector)
            if await btn.count() > 0:
                await btn.first.click()
                await asyncio.sleep(0.5)
                return True
    except Exception:
        pass
    return False


async def verify_subdomain(page, kabupaten: str) -> str | None:
    """Verify BPS subdomain works, try fallbacks if needed."""
    subdomain = BPS_SUBDOMAINS[kabupaten]
    url = f"https://{subdomain}.bps.go.id/id/publication"

    try:
        resp = await page.goto(url, wait_until="domcontentloaded", timeout=15000)
        if resp and resp.status < 400:
            await dismiss_popup(page)
            log.info(f"  ✓ Subdomain verified: {subdomain}.bps.go.id")
            return subdomain
    except Exception:
        pass

    # Try fallbacks
    for fb in BPS_SUBDOMAIN_FALLBACKS.get(kabupaten, []):
        url = f"https://{fb}.bps.go.id/id/publication"
        try:
            resp = await page.goto(url, wait_until="domcontentloaded", timeout=15000)
            if resp and resp.status < 400:
                await dismiss_popup(page)
                log.info(f"  ✓ Fallback subdomain works: {fb}.bps.go.id")
                BPS_SUBDOMAINS[kabupaten] = fb
                return fb
        except Exception:
            continue

    log.error(f"  ✖ No working subdomain for {kabupaten}")
    return None


async def search_publication(page, kecamatan: str, kabupaten: str) -> dict | None:
    """
    Search for a kecamatan publication on BPS.
    Tries 2025 first, then falls back to earlier years.
    Returns dict with publication info or None.
    """
    # Search keywords to try in order (some BPS sites index without "Kecamatan")
    search_variants = [
        lambda kec, yr: f"{kec} Dalam Angka {yr}",
        lambda kec, yr: f"Kecamatan {kec} Dalam Angka {yr}",
    ]

    for year in YEARS_TO_TRY:
        for make_keyword in search_variants:
            keyword = make_keyword(kecamatan, year)
            url = get_bps_url(kabupaten, keyword)

            log.info(f"    🔍 Searching: {keyword}")

            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=20000)
                await human_delay(DELAY_SHORT)
                await dismiss_popup(page)
            except PlaywrightTimeout:
                log.warning(f"    ⚠ Timeout loading search page")
                continue
            except Exception as e:
                log.warning(f"    ⚠ Error loading search: {e}")
                continue

            # Look for publication links in search results
            # The BPS page uses <a> tags with href containing /id/publication/
            pub_links = page.locator('a[href*="/id/publication/"]')
            count = await pub_links.count()

            if count == 0:
                log.info(f"    → No results")
                continue

            log.info(f"    → Found {count} result(s), checking titles...")

            # Find the best matching publication
            kec_lower = kecamatan.lower()
            for i in range(min(count, 10)):
                try:
                    link = pub_links.nth(i)
                    href = await link.get_attribute("href")

                    # Get ALL text in the link (may include description)
                    full_text = (await link.inner_text()).strip()

                    # The title is typically the first line or bold text
                    # Split by newline and check each line
                    lines = [l.strip() for l in full_text.split('\n') if l.strip()]

                    matched = False
                    matched_title = ""

                    # Strategy 1: Check each line individually
                    for line in lines:
                        line_lower = line.lower()
                        if kec_lower in line_lower and "dalam angka" in line_lower:
                            matched = True
                            matched_title = line
                            break

                    # Strategy 2: Check the full text
                    if not matched:
                        full_lower = full_text.lower()
                        if kec_lower in full_lower and "dalam angka" in full_lower:
                            matched = True
                            # Extract just the title part (first line usually)
                            matched_title = lines[0] if lines else full_text[:100]

                    if matched:
                        pub_url = href
                        if not href.startswith("http"):
                            pub_url = f"https://{BPS_SUBDOMAINS[kabupaten]}.bps.go.id{href}"
                        log.info(f"    ✓ Found: {matched_title}")
                        return {
                            "title": matched_title,
                            "url": pub_url,
                            "year": year,
                            "keyword": keyword,
                        }
                except Exception as e:
                    log.debug(f"    Error checking link {i}: {e}")
                    continue

            log.info(f"    → No matching title for {year}")

    return None


async def extract_pdf_link(page, pub_url: str) -> str | None:
    """Navigate to publication detail page and extract PDF download link."""
    try:
        await page.goto(pub_url, wait_until="domcontentloaded", timeout=20000)
        await human_delay(DELAY_SHORT)
        await dismiss_popup(page)
    except Exception as e:
        log.warning(f"    ⚠ Error loading publication page: {e}")
        return None

    # Strategy 1: Look for "Unduh Publikasi" button/link
    download_btn = page.locator('a:has-text("Unduh Publikasi")')
    if await download_btn.count() > 0:
        href = await download_btn.first.get_attribute("href")
        if href:
            return href if href.startswith("http") else f"https:{href}" if href.startswith("//") else href

    # Strategy 2: Look for download button by class or common patterns
    download_selectors = [
        'a[href*="download"]',
        'a[href*=".pdf"]',
        'a.btn-download',
        'button:has-text("Unduh")',
        'a:has-text("Download")',
    ]
    for selector in download_selectors:
        try:
            el = page.locator(selector)
            if await el.count() > 0:
                href = await el.first.get_attribute("href")
                if href:
                    return href if href.startswith("http") else href
        except Exception:
            continue

    # Strategy 3: Click the download button and intercept the download
    try:
        btn = page.locator('a:has-text("Unduh"), button:has-text("Unduh")')
        if await btn.count() > 0:
            # Set up download handler
            async with page.expect_download(timeout=15000) as download_info:
                await btn.first.click()
            download = await download_info.value
            return download.url
    except Exception:
        pass

    # Strategy 4: Look for PDF link in page source
    try:
        content = await page.content()
        pdf_match = re.search(r'href=["\']([^"\']*\.pdf[^"\']*)["\']', content, re.IGNORECASE)
        if pdf_match:
            return pdf_match.group(1)
    except Exception:
        pass

    log.warning("    ⚠ Could not extract PDF download link")
    return None


async def download_pdf(page, pdf_url: str, filename: str) -> bool:
    """Download a PDF file using the browser context."""
    filepath = PDF_DIR / filename
    if filepath.exists():
        log.info(f"    📁 Already downloaded: {filename}")
        return True

    try:
        # Use page to navigate and trigger download
        async with page.expect_download(timeout=60000) as download_info:
            await page.goto(pdf_url, wait_until="commit", timeout=30000)
        download = await download_info.value
        await download.save_as(str(filepath))
        log.info(f"    ✅ Downloaded: {filename} ({filepath.stat().st_size / 1024:.0f} KB)")
        return True
    except Exception:
        pass

    # Fallback: try using a new page for download
    try:
        import urllib.request
        req = urllib.request.Request(pdf_url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
        with urllib.request.urlopen(req, timeout=60) as response:
            with open(filepath, "wb") as f:
                f.write(response.read())
        log.info(f"    ✅ Downloaded (urllib): {filename} ({filepath.stat().st_size / 1024:.0f} KB)")
        return True
    except Exception as e:
        log.error(f"    ✖ Download failed: {e}")
        return False


# ─── Main ────────────────────────────────────────────────────────────────────


async def main():
    """Main scraping orchestrator."""
    log.info("=" * 65)
    log.info("  BPS Kecamatan Dalam Angka Report Scraper")
    log.info(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"  Total kecamatan: {len(KECAMATAN_LIST)}")
    log.info("=" * 65)

    # Create output directories
    SCRIPT_DIR.mkdir(parents=True, exist_ok=True)
    PDF_DIR.mkdir(parents=True, exist_ok=True)

    # Load existing progress for resume
    existing = load_existing_csv()
    if existing:
        log.info(f"  📂 Resuming: {len(existing)} already completed")

    all_records = []
    # Pre-populate with existing successful records
    for kec, row in existing.items():
        all_records.append(row)

    verified_subdomains = set()

    async with Stealth().use_async(async_playwright()) as p:
        browser = await p.chromium.launch(
            headless=False,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        context = await browser.new_context(
            viewport={"width": VIEWPORT_WIDTH, "height": VIEWPORT_HEIGHT},
            locale="id-ID",
            timezone_id="Asia/Makassar",
            accept_downloads=True,
        )
        page = await context.new_page()
        await page.set_extra_http_headers({
            "Accept-Language": "id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7",
        })

        for idx, (kecamatan, kabupaten) in enumerate(KECAMATAN_LIST, 1):
            log.info(f"\n{'─' * 65}")
            log.info(f"  [{idx}/{len(KECAMATAN_LIST)}] Kecamatan {kecamatan} — Kab. {kabupaten.title()}")

            # Skip if already done
            if kecamatan in existing:
                log.info(f"  ⏭ Already completed, skipping.")
                continue

            # Verify subdomain once per kabupaten
            if kabupaten not in verified_subdomains:
                subdomain = await verify_subdomain(page, kabupaten)
                if subdomain:
                    verified_subdomains.add(kabupaten)
                else:
                    record = {
                        "kecamatan": kecamatan, "kabupaten": kabupaten.title(),
                        "search_keyword": "", "year_found": "",
                        "publication_title": "", "publication_url": "",
                        "pdf_download_url": "", "pdf_filename": "",
                        "status": "error",
                        "error_message": f"BPS subdomain not found for {kabupaten}",
                    }
                    all_records.append(record)
                    save_csv(all_records)
                    continue

            # Search for publication
            pub_info = await search_publication(page, kecamatan, kabupaten)

            if not pub_info:
                log.warning(f"  ✖ No publication found for Kecamatan {kecamatan}")
                record = {
                    "kecamatan": kecamatan, "kabupaten": kabupaten.title(),
                    "search_keyword": f"Kecamatan {kecamatan} Dalam Angka",
                    "year_found": "", "publication_title": "",
                    "publication_url": "", "pdf_download_url": "",
                    "pdf_filename": "", "status": "not_found",
                    "error_message": "No matching publication found",
                }
                all_records.append(record)
                save_csv(all_records)
                await human_delay(DELAY_BETWEEN_PAGES)
                continue

            # Extract PDF download link from detail page
            pdf_url = await extract_pdf_link(page, pub_info["url"])

            # Build filename
            safe_name = sanitize_filename(kecamatan)
            pdf_filename = f"Kec_{safe_name}_Dalam_Angka_{pub_info['year']}.pdf"

            # Download PDF if we have a link
            downloaded = False
            if pdf_url:
                downloaded = await download_pdf(page, pdf_url, pdf_filename)

            record = {
                "kecamatan": kecamatan,
                "kabupaten": kabupaten.title(),
                "search_keyword": pub_info["keyword"],
                "year_found": pub_info["year"],
                "publication_title": pub_info["title"],
                "publication_url": pub_info["url"],
                "pdf_download_url": pdf_url or "",
                "pdf_filename": pdf_filename if downloaded else "",
                "status": "success" if pdf_url else "link_only",
                "error_message": "" if pdf_url else "PDF link not extracted",
            }
            all_records.append(record)
            save_csv(all_records)

            # Rate limiting
            await human_delay(DELAY_BETWEEN_PAGES)

        await browser.close()

    # ─── Summary ─────────────────────────────────────────────────────────
    log.info("\n" + "=" * 65)
    log.info("  SCRAPING SUMMARY")
    log.info("=" * 65)

    success = sum(1 for r in all_records if r.get("status") == "success")
    link_only = sum(1 for r in all_records if r.get("status") == "link_only")
    not_found = sum(1 for r in all_records if r.get("status") == "not_found")
    errors = sum(1 for r in all_records if r.get("status") == "error")

    log.info(f"  Total processed : {len(all_records)}")
    log.info(f"  ✅ Success       : {success}")
    log.info(f"  🔗 Link only     : {link_only}")
    log.info(f"  ❌ Not found     : {not_found}")
    log.info(f"  ⚠ Errors        : {errors}")
    log.info(f"  CSV output      : {CSV_OUTPUT}")
    log.info(f"  PDF directory   : {PDF_DIR}")
    log.info(f"  Completed       : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 65)


if __name__ == "__main__":
    asyncio.run(main())
