"""
Google Maps Scraper — Kalimantan Selatan Cash Handling Nodes
============================================================
Extracts location data (name, lat, lng, category) for potential cash handling
and distribution points across South Kalimantan (Kalimantan Selatan).

Target queries: Pos Indonesia, Pegadaian, Bank BRI, Bank BCA, Bank Mandiri,
                Bank BNI, Bank BTN, Bank Kalsel, Bank Syariah Indonesia,
                BRILink, Indomaret, Alfamart, MR DIY

Uses Playwright (headed + stealth) to interact with Google Maps SPA,
handles infinite scroll, extracts coordinates from URLs, and exports
a cleaned CSV via pandas.

Usage:
    pip install -r requirements.txt
    playwright install chromium
    python scrape_gmaps.py
"""

import asyncio
import re
import random
import logging
from datetime import datetime
from urllib.parse import quote, unquote

import pandas as pd
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
from playwright_stealth import Stealth

# ─── Configuration ───────────────────────────────────────────────────────────

# Target business types to search
SEARCH_QUERIES = [
    "Pos Indonesia",
    "Pegadaian",
    "Bank BRI",
    "Bank BCA",
    "Bank Mandiri",
    "Bank BNI",
    "Bank BTN",
    "Bank Kalsel",
    "Bank Syariah Indonesia",
    "BRILink",
    "Indomaret",
    "Alfamart",
    "MR DIY",
]

# Kalimantan Selatan (South Kalimantan) geographic center & bounding box
KALSEL_CENTER_LAT = -3.32
KALSEL_CENTER_LNG = 115.44
KALSEL_ZOOM = 9  # wider zoom to cover the larger province

# Bounding box to filter out-of-province results
KALSEL_LAT_MIN = -4.20
KALSEL_LAT_MAX = -1.30
KALSEL_LNG_MIN = 114.30
KALSEL_LNG_MAX = 116.60

# Anti-bot delay ranges (seconds)
DELAY_BETWEEN_CLICKS = (1.5, 3.5)
DELAY_BETWEEN_QUERIES = (4.0, 8.0)
DELAY_SCROLL_PAUSE = (1.0, 2.5)

# Maximum scroll attempts before giving up on loading more results
MAX_SCROLL_ATTEMPTS = 40

# Output file
OUTPUT_CSV = "kalsel_cash_nodes.csv"

# Browser viewport (realistic desktop)
VIEWPORT_WIDTH = 1366
VIEWPORT_HEIGHT = 768

# ─── Logging Setup ───────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-7s │ %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("gmaps_scraper")

# ─── Helper Functions ────────────────────────────────────────────────────────


def human_delay(delay_range: tuple[float, float]):
    """Sleep for a randomized human-like duration."""
    return asyncio.sleep(random.uniform(*delay_range))


def extract_coords_from_url(url: str) -> tuple[float | None, float | None]:
    """
    Extract latitude and longitude from Google Maps URL.
    Strategy 1: .../@-7.7956,110.3695,...
    Strategy 2: ...!3d-7.7956!4d110.3695... (data parameters)
    """
    # Strategy 1: @lat,lng pattern
    match = re.search(r"@(-?\d+\.\d+),(-?\d+\.\d+)", url)
    if match:
        return float(match.group(1)), float(match.group(2))

    # Strategy 2: !3d (latitude) and !4d (longitude) data params
    lat_match = re.search(r"!3d(-?\d+\.\d+)", url)
    lng_match = re.search(r"!4d(-?\d+\.\d+)", url)
    if lat_match and lng_match:
        return float(lat_match.group(1)), float(lng_match.group(1))

    return None, None


def extract_coords_from_html(html: str) -> tuple[float | None, float | None]:
    """
    Extract coordinates from the raw page HTML.
    Google Maps embeds coordinate data in JSON structures within the page source.
    """
    # Pattern: looks for coordinate pairs in the page source
    # Common patterns: [null,null,-3.3167,115.4900]  or  [[-3.3167,115.4900],...]
    # We search for patterns like ,-1...-4.XXXXX,114...116.XXXXX (Kalsel-range coords)
    matches = re.findall(r"(-[1-4]\.\d{3,8}),\s*(11[4-6]\.\d{3,8})", html)
    if matches:
        # Return the first match (usually the most prominent/primary coordinate)
        return float(matches[0][0]), float(matches[0][1])

    # Alternative pattern: coordinates in separate fields
    matches = re.findall(r"\[(-[1-4]\.\d{3,8}),\s*(11[4-6]\.\d{3,8})\]", html)
    if matches:
        return float(matches[0][0]), float(matches[0][1])

    return None, None


def is_within_kalsel(lat: float | None, lng: float | None) -> bool:
    """Check if coordinates fall within the Kalimantan Selatan bounding box."""
    if lat is None or lng is None:
        return False
    return (KALSEL_LAT_MIN <= lat <= KALSEL_LAT_MAX) and (KALSEL_LNG_MIN <= lng <= KALSEL_LNG_MAX)


def build_search_url(query: str) -> str:
    """Build a Google Maps search URL centered on Kalimantan Selatan."""
    encoded_query = quote(f"{query} Kalimantan Selatan")
    return (
        f"https://www.google.com/maps/search/{encoded_query}"
        f"/@{KALSEL_CENTER_LAT},{KALSEL_CENTER_LNG},{KALSEL_ZOOM}z"
        f"?hl=id"
    )


# ─── Coordinate Extraction (Multi-Strategy) ─────────────────────────────────


async def extract_coordinates(page, href: str) -> tuple[float | None, float | None]:
    """
    Multi-strategy coordinate extraction. Tries multiple methods in order:
    1. Parse @lat,lng from the current page URL
    2. Poll the URL for up to 3 seconds waiting for coordinates to appear
    3. Parse !3d / !4d parameters from the original href
    4. Extract from the raw page HTML source
    """
    # Strategy 1: immediate URL check
    lat, lng = extract_coords_from_url(page.url)
    if lat is not None:
        return lat, lng

    # Strategy 2: poll URL for coordinate update (Maps updates URL asynchronously)
    for _ in range(6):
        await asyncio.sleep(0.5)
        lat, lng = extract_coords_from_url(page.url)
        if lat is not None:
            return lat, lng

    # Strategy 3: parse from original href (!3d / !4d data params)
    lat, lng = extract_coords_from_url(href)
    if lat is not None:
        return lat, lng

    # Strategy 4: extract from page HTML source
    try:
        html = await page.content()
        lat, lng = extract_coords_from_html(html)
        if lat is not None:
            return lat, lng
    except Exception:
        pass

    # Strategy 5: try to get from the "Share" or coordinate display
    try:
        # Some place pages show coordinates in the address/info section
        # Look for an element with "data-tooltip" containing coordinate-like text
        info_section = await page.evaluate("""
            () => {
                // Look for coordinate-like text in the page
                const allText = document.body.innerText;
                const coordMatch = allText.match(/(-[1-4]\\.\\d{3,8}),\\s*(11[4-6]\\.\\d{3,8})/);
                if (coordMatch) return [parseFloat(coordMatch[1]), parseFloat(coordMatch[2])];

                // Look for coordinate data in meta tags
                const metas = document.querySelectorAll('meta[content]');
                for (const meta of metas) {
                    const content = meta.getAttribute('content');
                    const m = content.match(/(-[1-4]\\.\\d{3,8}),\\s*(11[4-6]\\.\\d{3,8})/);
                    if (m) return [parseFloat(m[1]), parseFloat(m[2])];
                }
                return null;
            }
        """)
        if info_section:
            return info_section[0], info_section[1]
    except Exception:
        pass

    return None, None


# ─── Core Scraping Functions ────────────────────────────────────────────────


async def scroll_results_feed(page) -> int:
    """
    Scroll the Google Maps results feed until all results are loaded.
    Returns the final count of result items found.
    """
    feed = page.locator('div[role="feed"]')

    try:
        await feed.wait_for(state="visible", timeout=10000)
    except PlaywrightTimeout:
        log.warning("Results feed not found — possibly no results for this query.")
        return 0

    previous_count = 0
    stale_rounds = 0

    for attempt in range(MAX_SCROLL_ATTEMPTS):
        # Count current result items (each is an <a> with a href to /maps/place/)
        items = feed.locator(':scope > div > div > a[href*="/maps/place/"]')
        current_count = await items.count()

        if current_count > previous_count:
            log.info(f"  ↳ Scroll #{attempt + 1}: {current_count} results loaded")
            previous_count = current_count
            stale_rounds = 0
        else:
            stale_rounds += 1

        # Check if we've hit the end-of-list marker (Indonesian and English)
        end_of_list = await page.evaluate("""
            () => {
                const feed = document.querySelector('div[role="feed"]');
                if (!feed) return false;
                const text = feed.innerText;
                return text.includes('Anda telah melihat semua hasil') ||
                       text.includes("You've reached the end of the list") ||
                       text.includes('Tidak ada hasil lagi');
            }
        """)

        if end_of_list:
            log.info(f"  ✓ End of results reached. Total: {current_count}")
            break

        if stale_rounds >= 5:
            log.info(f"  ✓ No new results after {stale_rounds} scrolls. Total: {current_count}")
            break

        # Scroll the feed container down
        await feed.evaluate("el => el.scrollTop = el.scrollHeight")
        await human_delay(DELAY_SCROLL_PAUSE)

    return previous_count


async def scrape_query(page, query: str) -> list[dict]:
    """
    Run a single search query on Google Maps and extract all results.
    """
    results = []
    url = build_search_url(query)

    log.info(f"{'═' * 60}")
    log.info(f"🔍 Searching: {query}")
    log.info(f"   URL: {url}")

    await page.goto(url, wait_until="domcontentloaded")
    await human_delay((3.0, 5.0))

    # Handle possible consent screen / cookie popup
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

    # Wait for the page to settle
    await human_delay((2.0, 3.0))

    # Scroll to load all results
    result_count = await scroll_results_feed(page)

    if result_count == 0:
        log.warning(f"   No results found for '{query}'.")
        return results

    # Collect all result link hrefs from the feed
    feed = page.locator('div[role="feed"]')
    links = feed.locator(':scope > div > div > a[href*="/maps/place/"]')
    total_links = await links.count()
    log.info(f"   Found {total_links} result links to process.")

    hrefs = []
    for i in range(total_links):
        try:
            href = await links.nth(i).get_attribute("href")
            if href:
                hrefs.append(href)
        except Exception:
            continue

    # Deduplicate hrefs while preserving order
    seen_hrefs = set()
    unique_hrefs = []
    for h in hrefs:
        if h not in seen_hrefs:
            seen_hrefs.add(h)
            unique_hrefs.append(h)

    log.info(f"   {len(unique_hrefs)} unique locations to extract.")

    for idx, href in enumerate(unique_hrefs, 1):
        log.info(f"   [{idx}/{len(unique_hrefs)}] Processing...")

        try:
            # Navigate directly to the place URL
            await page.goto(href, wait_until="domcontentloaded")
            await human_delay(DELAY_BETWEEN_CLICKS)

            # Wait for the detail panel heading to appear
            try:
                await page.wait_for_selector(
                    'h1.DUwDvf, [role="main"] h1', timeout=8000
                )
            except PlaywrightTimeout:
                log.warning(
                    f"    ⚠ Place detail didn't load for link #{idx}, skipping."
                )
                continue

            # ── Extract Name ──
            name = None
            name_el = page.locator("h1.DUwDvf")
            if await name_el.count() > 0:
                name = (await name_el.first.inner_text()).strip()
            else:
                name_el = page.locator('[role="main"] h1')
                if await name_el.count() > 0:
                    name = (await name_el.first.inner_text()).strip()

            if not name:
                log.warning(f"    ⚠ No name found, skipping.")
                continue

            # ── Extract Coordinates (multi-strategy) ──
            lat, lng = await extract_coordinates(page, href)

            # ── Extract Category ──
            category = None

            # Strategy 1: category button with jsaction
            cat_btn = page.locator('button[jsaction*="category"]')
            if await cat_btn.count() > 0:
                category = (await cat_btn.first.inner_text()).strip()

            # Strategy 2: DkEaL span (common category class)
            if not category:
                cat_span = page.locator("span.DkEaL")
                if await cat_span.count() > 0:
                    category = (await cat_span.first.inner_text()).strip()

            # Strategy 3: look in the info section for category-like text
            if not category:
                try:
                    category = await page.evaluate("""
                        () => {
                            // Look for category button or link near the title
                            const btns = document.querySelectorAll(
                                'button[jsaction*="category"], [data-item-id="authority"]'
                            );
                            for (const btn of btns) {
                                const text = btn.innerText.trim();
                                if (text && text.length < 50) return text;
                            }
                            return null;
                        }
                    """)
                except Exception:
                    pass

            # Fallback: use the search query itself
            if not category:
                category = query

            coord_str = f"({lat}, {lng})" if lat is not None else "(no coords)"
            log.info(f"    ✓ {name}  {coord_str}  [{category}]")

            results.append(
                {
                    "name": name,
                    "latitude": lat,
                    "longitude": lng,
                    "category": category,
                    "search_query": query,
                }
            )

        except Exception as e:
            log.warning(f"    ⚠ Error on link #{idx}: {e}")
            continue

        # Anti-bot delay between results
        await human_delay(DELAY_BETWEEN_CLICKS)

    return results


# ─── Main Entry Point ────────────────────────────────────────────────────────


async def main():
    """Main scraping orchestrator."""
    log.info("=" * 60)
    log.info("  Google Maps Scraper — Kalimantan Selatan Cash Handling Nodes")
    log.info(f"  Started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"  Queries: {', '.join(SEARCH_QUERIES)}")
    log.info(f"  Output:  {OUTPUT_CSV}")
    log.info("=" * 60)

    all_results: list[dict] = []

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

        # Set extra headers for realism
        await page.set_extra_http_headers(
            {
                "Accept-Language": "id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7",
            }
        )

        for query_idx, query in enumerate(SEARCH_QUERIES, 1):
            log.info(f"\n📋 Query {query_idx}/{len(SEARCH_QUERIES)}: {query}")

            try:
                results = await scrape_query(page, query)
                all_results.extend(results)
                log.info(f"   ➤ Collected {len(results)} results for '{query}'")
            except Exception as e:
                log.error(f"   ✖ Failed on query '{query}': {e}")

            # Delay between queries
            if query_idx < len(SEARCH_QUERIES):
                delay = random.uniform(*DELAY_BETWEEN_QUERIES)
                log.info(f"   ⏳ Waiting {delay:.1f}s before next query...")
                await asyncio.sleep(delay)

        await browser.close()

    # ─── Data Cleaning & Export ──────────────────────────────────────────────

    log.info("\n" + "=" * 60)
    log.info("📊 Post-processing results...")

    if not all_results:
        log.warning("No results collected! CSV will not be created.")
        return

    df = pd.DataFrame(all_results)
    log.info(f"   Raw records: {len(df)}")

    # Drop rows with missing coordinates
    before = len(df)
    df = df.dropna(subset=["latitude", "longitude"])
    dropped_null = before - len(df)
    if dropped_null > 0:
        log.info(f"   Dropped {dropped_null} rows with null coordinates.")

    # Filter to Kalimantan Selatan bounding box
    before = len(df)
    df = df[
        (df["latitude"] >= KALSEL_LAT_MIN)
        & (df["latitude"] <= KALSEL_LAT_MAX)
        & (df["longitude"] >= KALSEL_LNG_MIN)
        & (df["longitude"] <= KALSEL_LNG_MAX)
    ]
    dropped_bbox = before - len(df)
    if dropped_bbox > 0:
        log.info(f"   Dropped {dropped_bbox} rows outside Kalimantan Selatan bounding box.")

    # Deduplicate by name + coordinates
    before = len(df)
    df = df.drop_duplicates(subset=["name", "latitude", "longitude"], keep="first")
    dropped_dupes = before - len(df)
    if dropped_dupes > 0:
        log.info(f"   Dropped {dropped_dupes} duplicate entries.")

    # Sort by search_query then name
    df = df.sort_values(["search_query", "name"]).reset_index(drop=True)

    # Select final columns
    df_export = df[["name", "latitude", "longitude", "category", "search_query"]]

    # Export to CSV
    df_export.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    log.info(f"\n   ✅ Exported {len(df_export)} records to {OUTPUT_CSV}")

    # Print summary by query
    log.info("\n   Summary by search query:")
    summary = df_export.groupby("search_query").size()
    for q, count in summary.items():
        log.info(f"     • {q}: {count} locations")

    log.info(f"\n{'═' * 60}")
    log.info(
        f"  Scraping completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    log.info(f"{'═' * 60}")


if __name__ == "__main__":
    asyncio.run(main())
