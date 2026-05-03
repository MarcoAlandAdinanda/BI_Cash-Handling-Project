"""
Government Office Scraper — Kelurahan Kalimantan Selatan
========================================================
Searches Google Maps for the government office (Kantor Kelurahan / Kantor Desa /
Balai Desa) for each kelurahan listed in `goverment_office_nodes_template.csv`.

Extracts:
  - latitude, longitude
  - street_name  (from the address line on the Maps detail panel)
  - gmaps_link   (https://www.google.com/maps?q=lat,lng)
  - location_name_maps       (actual name shown on Google Maps)
  - location_name_structured  (Gedung_Kelurahan_{NAME})

Features:
  - Resume capability: saves progress after every SAVE_EVERY batch
  - Multiple search fallbacks (Kantor Kelurahan → Kantor Desa → Balai Desa)
  - Anti-bot: stealth plugin, human-like delays, realistic browser context

Usage:
    pip install -r requirements.txt
    playwright install chromium
    python goverment_office/scrape_goverment_office.py
"""

import asyncio
import csv
import os
import re
import random
import logging
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

import pandas as pd
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
from playwright_stealth import Stealth

# ─── Paths ───────────────────────────────────────────────────────────────────

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

TEMPLATE_CSV = PROJECT_ROOT / "goverment_office_nodes_template.csv"
OUTPUT_CSV = SCRIPT_DIR / "goverment_office_nodes.csv"
PROGRESS_CSV = SCRIPT_DIR / "goverment_office_nodes_progress.csv"
LOG_FILE = SCRIPT_DIR / "scrape_log.txt"

# ─── Configuration ───────────────────────────────────────────────────────────

# Kalimantan Selatan geographic parameters
KALSEL_CENTER_LAT = -3.32
KALSEL_CENTER_LNG = 115.44
KALSEL_ZOOM = 14

# Bounding box for coordinate validation
KALSEL_LAT_MIN = -4.20
KALSEL_LAT_MAX = -1.30
KALSEL_LNG_MIN = 114.30
KALSEL_LNG_MAX = 116.60

# Anti-bot delays (seconds)
DELAY_BETWEEN_SEARCHES = (3.0, 6.0)
DELAY_PAGE_LOAD = (2.0, 4.0)
DELAY_POLL = 0.5

# Save progress every N kelurahan
SAVE_EVERY = 10

# Browser viewport
VIEWPORT_WIDTH = 1366
VIEWPORT_HEIGHT = 768

# ─── Logging ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-7s │ %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger("gov_office_scraper")

# ─── Helpers ─────────────────────────────────────────────────────────────────


def human_delay(delay_range: tuple[float, float]):
    """Sleep for a randomized human-like duration."""
    return asyncio.sleep(random.uniform(*delay_range))


def make_structured_name(kelurahan: str) -> str:
    """
    Generate the structured location name: Gedung_Kelurahan_{NAME}
    Spaces are replaced with underscores, special chars cleaned.
    """
    clean = kelurahan.strip()
    clean = re.sub(r"[^\w\s]", "", clean)          # remove special chars
    clean = re.sub(r"\s+", "_", clean)              # spaces → underscores
    return f"Gedung_Kelurahan_{clean}"


def build_search_url(query: str) -> str:
    """Build a Google Maps search URL."""
    encoded = quote(query)
    return (
        f"https://www.google.com/maps/search/{encoded}"
        f"/@{KALSEL_CENTER_LAT},{KALSEL_CENTER_LNG},{KALSEL_ZOOM}z"
        f"?hl=id"
    )


def generate_search_queries(kelurahan: str, kecamatan: str) -> list[str]:
    """
    Generate a prioritized list of search queries for a kelurahan.
    Tries multiple phrasings to maximize hit rate.
    """
    area = f"{kecamatan}, Kalimantan Selatan"
    return [
        f"Kantor Kelurahan {kelurahan} {area}",
        f"Kantor Desa {kelurahan} {area}",
        f"Balai Desa {kelurahan} {area}",
        f"Kelurahan {kelurahan} {area}",
        f"Kantor Lurah {kelurahan} {area}",
    ]


# ─── Coordinate Extraction ──────────────────────────────────────────────────


def extract_coords_from_url(url: str) -> tuple[float | None, float | None]:
    """Extract lat/lng from Google Maps URL patterns."""
    # @lat,lng pattern
    match = re.search(r"@(-?\d+\.\d+),(-?\d+\.\d+)", url)
    if match:
        return float(match.group(1)), float(match.group(2))

    # !3d (lat) and !4d (lng) data params
    lat_m = re.search(r"!3d(-?\d+\.\d+)", url)
    lng_m = re.search(r"!4d(-?\d+\.\d+)", url)
    if lat_m and lng_m:
        return float(lat_m.group(1)), float(lng_m.group(1))

    return None, None


def extract_coords_from_html(html: str) -> tuple[float | None, float | None]:
    """Extract coordinates from page HTML (JSON-embedded data)."""
    matches = re.findall(r"(-[1-4]\.\d{3,8}),\s*(11[4-6]\.\d{3,8})", html)
    if matches:
        return float(matches[0][0]), float(matches[0][1])

    matches = re.findall(r"\[(-[1-4]\.\d{3,8}),\s*(11[4-6]\.\d{3,8})\]", html)
    if matches:
        return float(matches[0][0]), float(matches[0][1])

    return None, None


def is_within_kalsel(lat: float | None, lng: float | None) -> bool:
    """Validate coordinates are within Kalimantan Selatan."""
    if lat is None or lng is None:
        return False
    return (KALSEL_LAT_MIN <= lat <= KALSEL_LAT_MAX) and (
        KALSEL_LNG_MIN <= lng <= KALSEL_LNG_MAX
    )


async def extract_coordinates(page, href: str) -> tuple[float | None, float | None]:
    """
    Multi-strategy coordinate extraction:
    1. Current page URL (@lat,lng)
    2. Poll URL for async updates
    3. Original href (!3d/!4d params)
    4. Page HTML source
    5. Page text / meta tags
    """
    # Strategy 1: immediate URL
    lat, lng = extract_coords_from_url(page.url)
    if lat is not None and is_within_kalsel(lat, lng):
        return lat, lng

    # Strategy 2: poll URL
    for _ in range(6):
        await asyncio.sleep(DELAY_POLL)
        lat, lng = extract_coords_from_url(page.url)
        if lat is not None and is_within_kalsel(lat, lng):
            return lat, lng

    # Strategy 3: original href
    lat, lng = extract_coords_from_url(href)
    if lat is not None and is_within_kalsel(lat, lng):
        return lat, lng

    # Strategy 4: page HTML
    try:
        html = await page.content()
        lat, lng = extract_coords_from_html(html)
        if lat is not None and is_within_kalsel(lat, lng):
            return lat, lng
    except Exception:
        pass

    # Strategy 5: page text / meta
    try:
        coords = await page.evaluate("""
            () => {
                const allText = document.body.innerText;
                const m = allText.match(/(-[1-4]\\.\\d{3,8}),\\s*(11[4-6]\\.\\d{3,8})/);
                if (m) return [parseFloat(m[1]), parseFloat(m[2])];

                const metas = document.querySelectorAll('meta[content]');
                for (const meta of metas) {
                    const c = meta.getAttribute('content');
                    const mm = c.match(/(-[1-4]\\.\\d{3,8}),\\s*(11[4-6]\\.\\d{3,8})/);
                    if (mm) return [parseFloat(mm[1]), parseFloat(mm[2])];
                }
                return null;
            }
        """)
        if coords:
            return coords[0], coords[1]
    except Exception:
        pass

    return None, None


# ─── Street Name Extraction ─────────────────────────────────────────────────


async def extract_street_name(page) -> str | None:
    """
    Extract the street / address from the Google Maps place detail panel.
    Tries several selectors commonly used by Google Maps.
    """
    selectors = [
        'button[data-item-id="address"] div.fontBodyMedium',
        'button[data-item-id="address"]',
        '[data-item-id="address"]',
        'div.rogA2c div.Io6YTe',        # address line
        'div.RcCsl div.rogA2c',           # alternative layout
    ]

    for sel in selectors:
        try:
            el = page.locator(sel)
            if await el.count() > 0:
                text = (await el.first.inner_text()).strip()
                if text and len(text) > 3:
                    return text
        except Exception:
            continue

    # Fallback: evaluate JS to grab address from the info section
    try:
        addr = await page.evaluate("""
            () => {
                // Look for the address button or div
                const addrBtn = document.querySelector(
                    'button[data-item-id="address"]'
                );
                if (addrBtn) {
                    const text = addrBtn.innerText.trim();
                    if (text) return text;
                }

                // Alternative: look for elements with "Jl." or "Jalan" text
                const allElements = document.querySelectorAll(
                    'div.fontBodyMedium, div.Io6YTe, span.Io6YTe'
                );
                for (const el of allElements) {
                    const t = el.innerText.trim();
                    if (t && (t.includes('Jl.') || t.includes('Jalan') ||
                              t.includes('Desa') || t.includes('Kec.'))) {
                        return t;
                    }
                }
                return null;
            }
        """)
        return addr
    except Exception:
        return None


# ─── Location Name Extraction ───────────────────────────────────────────────


async def extract_location_name(page) -> str | None:
    """Extract the location name from the Google Maps place detail heading."""
    try:
        name_el = page.locator("h1.DUwDvf")
        if await name_el.count() > 0:
            return (await name_el.first.inner_text()).strip()
    except Exception:
        pass

    try:
        name_el = page.locator('[role="main"] h1')
        if await name_el.count() > 0:
            return (await name_el.first.inner_text()).strip()
    except Exception:
        pass

    return None


# ─── Single Kelurahan Search ────────────────────────────────────────────────


async def search_kelurahan(
    page, kecamatan: str, kelurahan: str
) -> dict:
    """
    Search Google Maps for a kelurahan's government office.
    Tries multiple query variations. Returns a dict with extracted data.
    """
    queries = generate_search_queries(kelurahan, kecamatan)
    structured_name = make_structured_name(kelurahan)

    for query in queries:
        url = build_search_url(query)
        log.info(f"    🔍 Trying: {query}")

        try:
            await page.goto(url, wait_until="domcontentloaded")
            await human_delay(DELAY_PAGE_LOAD)

            # Check if we landed on a single place page (detail view)
            # vs. a search results feed
            is_detail = await page.locator("h1.DUwDvf").count() > 0

            if not is_detail:
                # We got a results list — try clicking the first result
                feed = page.locator('div[role="feed"]')
                try:
                    await feed.wait_for(state="visible", timeout=5000)
                except PlaywrightTimeout:
                    log.info(f"    ↳ No results feed for this query.")
                    continue

                first_link = feed.locator(
                    ':scope > div > div > a[href*="/maps/place/"]'
                )
                if await first_link.count() == 0:
                    log.info(f"    ↳ No place results found.")
                    continue

                # Get the href and navigate to it
                href = await first_link.first.get_attribute("href") or ""
                await first_link.first.click()
                await human_delay(DELAY_PAGE_LOAD)

                # Wait for detail panel
                try:
                    await page.wait_for_selector(
                        "h1.DUwDvf, [role='main'] h1", timeout=8000
                    )
                except PlaywrightTimeout:
                    log.info(f"    ↳ Detail panel didn't load.")
                    continue
            else:
                href = page.url

            # ── Extract data ──
            location_name_maps = await extract_location_name(page)
            if not location_name_maps:
                log.info(f"    ↳ No location name found, trying next query.")
                continue

            lat, lng = await extract_coordinates(page, href)
            street_name = await extract_street_name(page)
            gmaps_link = (
                f"https://www.google.com/maps?q={lat},{lng}"
                if lat is not None
                else ""
            )

            log.info(
                f"    ✓ Found: {location_name_maps}  "
                f"({lat}, {lng})  "
                f"Street: {street_name or 'N/A'}"
            )

            return {
                "Kecamatan": kecamatan,
                "Kelurahan": kelurahan,
                "latitude": lat,
                "longitude": lng,
                "street_name": street_name or "",
                "search_query": query,
                "gmaps_link": gmaps_link,
                "location_name_maps": location_name_maps,
                "location_name_structured": structured_name,
            }

        except Exception as e:
            log.warning(f"    ⚠ Error with query '{query}': {e}")
            continue

        await human_delay(DELAY_BETWEEN_SEARCHES)

    # All queries exhausted — not found
    log.warning(f"    ✖ Not found: {kelurahan} ({kecamatan})")
    return {
        "Kecamatan": kecamatan,
        "Kelurahan": kelurahan,
        "latitude": None,
        "longitude": None,
        "street_name": "",
        "search_query": "",
        "gmaps_link": "",
        "location_name_maps": "",
        "location_name_structured": structured_name,
    }


# ─── Progress / Resume ──────────────────────────────────────────────────────

OUTPUT_COLUMNS = [
    "Kecamatan",
    "Kelurahan",
    "latitude",
    "longitude",
    "street_name",
    "search_query",
    "gmaps_link",
    "location_name_maps",
    "location_name_structured",
]


def load_progress() -> pd.DataFrame:
    """Load previously saved progress, or return empty DataFrame."""
    if PROGRESS_CSV.exists():
        df = pd.read_csv(PROGRESS_CSV, sep=";", dtype=str)
        log.info(f"📂 Resuming: loaded {len(df)} completed rows from progress file.")
        return df
    return pd.DataFrame(columns=OUTPUT_COLUMNS)


def save_progress(df: pd.DataFrame):
    """Save current progress to the progress CSV."""
    df.to_csv(PROGRESS_CSV, sep=";", index=False, encoding="utf-8-sig")


def get_completed_keys(df: pd.DataFrame) -> set[tuple[str, str]]:
    """Return set of (Kecamatan, Kelurahan) tuples already completed."""
    if df.empty:
        return set()
    return set(zip(df["Kecamatan"].str.strip(), df["Kelurahan"].str.strip()))


# ─── Main ────────────────────────────────────────────────────────────────────


async def main():
    """Main scraping orchestrator."""
    log.info("=" * 65)
    log.info("  Government Office Scraper — Kelurahan Kalimantan Selatan")
    log.info(f"  Started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"  Template:  {TEMPLATE_CSV}")
    log.info(f"  Output:    {OUTPUT_CSV}")
    log.info("=" * 65)

    # ── Load template ──
    template = pd.read_csv(TEMPLATE_CSV, sep=";", dtype=str)
    # Keep only Kecamatan and Kelurahan columns (the rest are empty placeholders)
    template = template[["Kecamatan", "Kelurahan"]].dropna()
    total = len(template)
    log.info(f"📋 Total kelurahan to process: {total}")

    # ── Load progress ──
    progress_df = load_progress()
    completed = get_completed_keys(progress_df)
    log.info(f"✅ Already completed: {len(completed)}")

    # ── Filter remaining ──
    remaining = [
        row
        for _, row in template.iterrows()
        if (row["Kecamatan"].strip(), row["Kelurahan"].strip()) not in completed
    ]
    log.info(f"📝 Remaining to scrape: {len(remaining)}")

    if not remaining:
        log.info("🎉 All kelurahan already scraped! Generating final output.")
        progress_df.to_csv(OUTPUT_CSV, sep=";", index=False, encoding="utf-8-sig")
        log.info(f"   ✅ Final output saved to {OUTPUT_CSV}")
        return

    # ── Collect results ──
    new_results: list[dict] = []
    batch_counter = 0

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
            geolocation={
                "latitude": KALSEL_CENTER_LAT,
                "longitude": KALSEL_CENTER_LNG,
            },
            permissions=["geolocation"],
        )

        page = await context.new_page()
        await page.set_extra_http_headers(
            {"Accept-Language": "id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7"}
        )

        # Handle consent popup on first navigation
        await page.goto("https://www.google.com/maps/?hl=id", wait_until="domcontentloaded")
        await human_delay((3.0, 5.0))
        try:
            consent_btn = page.locator(
                'button:text("Accept all"), '
                'button:text("Terima semua"), '
                'form[action*="consent"] button'
            )
            if await consent_btn.count() > 0:
                await consent_btn.first.click()
                await human_delay((1.0, 2.0))
                log.info("   Dismissed consent popup.")
        except Exception:
            pass

        for idx, row in enumerate(remaining, 1):
            kecamatan = row["Kecamatan"].strip()
            kelurahan = row["Kelurahan"].strip()
            done_total = len(completed) + len(new_results)

            log.info(
                f"\n{'─' * 65}\n"
                f"  [{done_total + 1}/{total}] "
                f"Kec. {kecamatan} → Kel. {kelurahan}\n"
                f"{'─' * 65}"
            )

            result = await search_kelurahan(page, kecamatan, kelurahan)
            new_results.append(result)
            batch_counter += 1

            # ── Save progress periodically ──
            if batch_counter >= SAVE_EVERY:
                new_df = pd.DataFrame(new_results, columns=OUTPUT_COLUMNS)
                progress_df = pd.concat([progress_df, new_df], ignore_index=True)
                save_progress(progress_df)
                log.info(
                    f"💾 Progress saved: {len(progress_df)} total rows "
                    f"({len(new_results)} new in this batch)"
                )
                new_results.clear()
                batch_counter = 0

            # ── Anti-bot delay ──
            await human_delay(DELAY_BETWEEN_SEARCHES)

        await browser.close()

    # ── Save any remaining results ──
    if new_results:
        new_df = pd.DataFrame(new_results, columns=OUTPUT_COLUMNS)
        progress_df = pd.concat([progress_df, new_df], ignore_index=True)
        save_progress(progress_df)
        log.info(f"💾 Final progress saved: {len(progress_df)} total rows")

    # ── Generate final output ──
    progress_df.to_csv(OUTPUT_CSV, sep=";", index=False, encoding="utf-8-sig")

    # ── Summary ──
    found = progress_df[
        progress_df["latitude"].notna() & (progress_df["latitude"] != "")
    ]
    not_found = len(progress_df) - len(found)

    log.info(f"\n{'═' * 65}")
    log.info(f"  SCRAPING COMPLETE")
    log.info(f"  Total kelurahan:  {len(progress_df)}")
    log.info(f"  Found:            {len(found)}")
    log.info(f"  Not found:        {not_found}")
    log.info(f"  Output:           {OUTPUT_CSV}")
    log.info(f"  Finished at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"{'═' * 65}")


if __name__ == "__main__":
    asyncio.run(main())
