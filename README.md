<p align="center">
  <img src="assets/banner.png" alt="BI Cash Handling Project Banner" width="100%"/>
</p>

<h1 align="center">💰 BI Cash-Handling Distribution Node Mapper</h1>

<p align="center">
  <strong>Automated geospatial extraction of potential cash handling & distribution points across South Kalimantan (Kalimantan Selatan), Indonesia.</strong>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.11+-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python"/>
  <img src="https://img.shields.io/badge/Playwright-2F4F4F?style=for-the-badge&logo=playwright&logoColor=white" alt="Playwright"/>
  <img src="https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white" alt="Pandas"/>
  <img src="https://img.shields.io/badge/Data_Points-TBD-00C853?style=for-the-badge" alt="Data Points"/>
  <img src="https://img.shields.io/badge/License-Academic_Use-FFC107?style=for-the-badge" alt="License"/>
</p>

---

## 📋 Table of Contents

- [Overview](#-overview)
- [Motivation](#-motivation)
- [Architecture](#-architecture)
- [Data Schema](#-data-schema)
- [Scraped Results Summary](#-scraped-results-summary)
- [Getting Started](#-getting-started)
- [Configuration](#-configuration)
- [How It Works](#-how-it-works)
- [Output](#-output)
- [Project Structure](#-project-structure)
- [Limitations & Disclaimer](#-limitations--disclaimer)

---

## 🔍 Overview

This project provides an **automated web scraping pipeline** that extracts location data from **Google Maps** for businesses that can serve as proxy nodes in a cash handling and distribution network. Using browser automation with anti-detection measures, the scraper collects precise geospatial data — name, coordinates, and business category — for **13 search queries (7 target business categories)** across the Kalimantan Selatan province.

The extracted dataset (`kalsel_cash_nodes.csv`) is ready for downstream spatial analysis, clustering, route optimization, and geographic visualization.

---

## 💡 Motivation

Bank Indonesia's cash distribution network relies on identifying strategically located proxy points that have:

- ✅ Established **point-of-sale infrastructure**
- ✅ Secure, **commercially operated** environments
- ✅ High **geographic density** in both urban and semi-urban areas
- ✅ Extended **operating hours** for public accessibility

The following business types were selected as candidates:

| Business | Rationale |
|:---------|:----------|
| **Pos Indonesia** | National postal network with existing financial service capabilities |
| **Pegadaian** | State-owned pawn shops with cash-heavy transactions and vault infrastructure |
| **Commercial Banks** (BRI, BCA, Mandiri, BNI, BTN, Bank Kalsel, BSI) | Core banking institutions with established cash handling infrastructure |
| **BRILink** | BRI's branchless banking agent network with wide rural penetration |
| **Indomaret** | Largest minimarket chain; POS systems, digital payment integration |
| **Alfamart** | Second-largest minimarket chain; comparable infrastructure to Indomaret |
| **MR DIY** | Growing retail chain with established point-of-sale infrastructure |

---

## 🏗 Architecture

The scraping pipeline follows a three-stage architecture:

```mermaid
graph LR
    A["🔍 Search & Scroll"] --> B["📍 Extract Details"]
    B --> C["📊 Clean & Export"]

    subgraph "Stage 1: Discovery"
        A1["Navigate to Google Maps\n(geo-centered on Kalsel)"] --> A2["Execute search query\n(e.g. 'Indomaret Kalimantan Selatan')"]
        A2 --> A3["Scroll results feed\nuntil end-of-list"]
        A3 --> A4["Collect all unique\nplace URLs"]
    end

    subgraph "Stage 2: Extraction"
        B1["Visit each place URL"] --> B2["Extract name from\nh1 heading"]
        B2 --> B3["Extract coordinates\n(5-strategy fallback)"]
        B3 --> B4["Extract category\nfrom detail panel"]
    end

    subgraph "Stage 3: Post-Processing"
        C1["Build DataFrame\n(pandas)"] --> C2["Drop null coordinates\n& filter to Kalsel bbox"]
        C2 --> C3["Deduplicate by\n(name, lat, lng)"]
        C3 --> C4["Export to CSV\n(UTF-8 BOM)"]
    end
```

### Multi-Strategy Coordinate Extraction

Google Maps does not always expose coordinates in the URL immediately. The scraper employs a **5-level fallback chain** to guarantee extraction:

```
Strategy 1 → Parse @lat,lng from current page URL
    ↓ (fail)
Strategy 2 → Poll the URL for up to 3 seconds (async update)
    ↓ (fail)
Strategy 3 → Parse !3d / !4d data parameters from the original href
    ↓ (fail)
Strategy 4 → Regex scan on raw page HTML source for Kalsel-range coordinates
    ↓ (fail)
Strategy 5 → JavaScript evaluation on page text content & meta tags
```

> This cascade is designed to achieve a high coordinate extraction rate across all search queries.

---

## 📐 Data Schema

The output CSV (`kalsel_cash_nodes.csv`) contains the following columns:

| Column | Type | Description | Example |
|:-------|:-----|:------------|:--------|
| `name` | `string` | Official business/branch name as displayed on Google Maps | `Indomaret Banjarmasin` |
| `latitude` | `float` | Latitudinal coordinate (WGS 84) | `-3.3167000` |
| `longitude` | `float` | Longitudinal coordinate (WGS 84) | `114.5900000` |
| `category` | `string` | Business type as categorized by Google Maps (in Indonesian) | `Minimarket` |
| `search_query` | `string` | The original search query that returned this result | `Indomaret` |

### Category Labels (Indonesian)

Since the scraper runs with `hl=id` (Indonesian locale), the category labels are returned in Indonesian:

| Indonesian Category | English Equivalent |
|:-------------------|:------------------|
| Minimarket | Convenience Store |
| Toko Gadai | Pawn Shop |
| Kantor Pos | Post Office |
| Bank | Bank |
| Agen Perbankan | Banking Agent |
| Toko Peralatan Rumah Tangga | Home Improvement Store |

---

## 📊 Scraped Results Summary

> ⏳ **Pending** — Run the scraper to populate results for Kalimantan Selatan.

### Target Search Queries

| Search Query | Category |
|:-------------|:---------|
| 📮 Pos Indonesia | Post Office |
| 💍 Pegadaian | Pawn Shop |
| 🏦 Bank BRI | Commercial Bank |
| 🏦 Bank BCA | Commercial Bank |
| 🏦 Bank Mandiri | Commercial Bank |
| 🏦 Bank BNI | Commercial Bank |
| 🏦 Bank BTN | Commercial Bank |
| 🏦 Bank Kalsel | Regional Bank |
| 🏦 Bank Syariah Indonesia | Islamic Bank |
| 💳 BRILink | Banking Agent |
| 🏪 Indomaret | Minimarket |
| 🏪 Alfamart | Minimarket |
| 🔧 MR DIY | Retail Store |

### Geographic Coverage

| Metric | Value |
|:-------|:------|
| Latitude range | `-4.20` to `-1.30` |
| Longitude range | `114.30` to `116.60` |
| Bounding box | Kalimantan Selatan province |

---

## 🚀 Getting Started

### Prerequisites

- **Python 3.11+**
- **Windows / macOS / Linux** with a display (headed browser mode)
- A stable internet connection

### Installation

```bash
# 1. Clone the repository
git clone https://github.com/MarcoAlandAdinanda/BI_Cash-Handling-Project.git
cd BI_Cash-Handling-Project

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Install Playwright's Chromium browser
playwright install chromium
```

### Run the Scraper

```bash
python scrape_gmaps.py
```

> ⏱ **Expected runtime:** ~60–90 minutes for all 13 queries. A visible Chromium browser window will open and operate autonomously.

---

## ⚙ Configuration

All tunable parameters are defined as constants at the top of `scrape_gmaps.py`:

### Search Parameters

| Constant | Default | Description |
|:---------|:--------|:------------|
| `SEARCH_QUERIES` | 13 queries | List of business types to search |
| `KALSEL_CENTER_LAT` | `-3.32` | Map center latitude (Kalimantan Selatan) |
| `KALSEL_CENTER_LNG` | `115.44` | Map center longitude |
| `KALSEL_ZOOM` | `9` | Google Maps zoom level |

### Bounding Box Filter

| Constant | Default | Description |
|:---------|:--------|:------------|
| `KALSEL_LAT_MIN` | `-4.20` | Southern boundary |
| `KALSEL_LAT_MAX` | `-1.30` | Northern boundary |
| `KALSEL_LNG_MIN` | `114.30` | Western boundary |
| `KALSEL_LNG_MAX` | `116.60` | Eastern boundary |

### Anti-Bot Timing

| Constant | Default | Description |
|:---------|:--------|:------------|
| `DELAY_BETWEEN_CLICKS` | `(1.5, 3.5)` sec | Random delay between place visits |
| `DELAY_BETWEEN_QUERIES` | `(4.0, 8.0)` sec | Random delay between search queries |
| `DELAY_SCROLL_PAUSE` | `(1.0, 2.5)` sec | Random delay between scroll actions |
| `MAX_SCROLL_ATTEMPTS` | `40` | Max scroll iterations per query |

### Output

| Constant | Default | Description |
|:---------|:--------|:------------|
| `OUTPUT_CSV` | `kalsel_cash_nodes.csv` | Output filename |

---

## 🔧 How It Works

### 1. Browser Initialization

The scraper launches a **headed Chromium** browser via Playwright with stealth patches applied to mask automation signals:

```python
async with Stealth().use_async(async_playwright()) as p:
    browser = await p.chromium.launch(
        headless=False,
        args=["--disable-blink-features=AutomationControlled"],
    )
    context = await browser.new_context(
        locale="id-ID",
        timezone_id="Asia/Makassar",
        geolocation={"latitude": -3.32, "longitude": 115.44},
    )
```

**Anti-detection features include:**
- `playwright-stealth` patches (`navigator.webdriver`, plugins, etc.)
- Indonesian locale and Makassar timezone (WITA)
- Kalimantan Selatan geolocation spoofing
- Realistic viewport (1366×768)
- Indonesian `Accept-Language` headers

### 2. Search & Infinite Scroll

For each query, the scraper:

1. Navigates to a pre-constructed URL centered on Kalimantan Selatan
2. Waits for the results `<div role="feed">` to appear
3. Incrementally scrolls the feed container, counting results after each scroll
4. Stops when: end-of-list marker is detected **OR** no new results after 5 consecutive scrolls

```python
# Scroll the feed container
await feed.evaluate("el => el.scrollTop = el.scrollHeight")
```

### 3. Per-Location Data Extraction

For each unique place URL collected:

- **Name** → Extracted from `h1.DUwDvf` or `[role="main"] h1`
- **Coordinates** → 5-strategy fallback cascade (see [Architecture](#-architecture))
- **Category** → Extracted from `button[jsaction*="category"]`, `span.DkEaL`, or JS evaluation

### 4. Data Cleaning Pipeline

```python
df.dropna(subset=["latitude", "longitude"])              # Remove null coordinates
df = df[(df.latitude >= LAT_MIN) & ...]                  # Filter to Kalsel bbox
df.drop_duplicates(subset=["name", "lat", "lng"])         # Deduplicate
df.to_csv("kalsel_cash_nodes.csv", encoding="utf-8-sig") # Export (Excel-friendly)
```

---

## 📁 Output

### CSV Preview

```
name,latitude,longitude,category,search_query
Indomaret Banjarmasin,-3.3167000,114.5900000,Minimarket,Indomaret
Pegadaian Banjarmasin,-3.3200000,114.5950000,Toko Gadai,Pegadaian
Bank BRI Banjarmasin,-3.3180000,114.5920000,Bank,Bank BRI
BRILink Agen Barabai,-2.5800000,115.3800000,Agen Perbankan,BRILink
Pos Indonesia Banjarmasin,-3.3150000,114.5880000,Kantor Pos,Pos Indonesia
```

### Using the Data

```python
import pandas as pd

df = pd.read_csv("kalsel_cash_nodes.csv")

# Filter only convenience stores
stores = df[df["category"] == "Minimarket"]

# Get all Bank BRI locations
bri = df[df["search_query"] == "Bank BRI"]

# Geographic center of all nodes
center_lat = df["latitude"].mean()
center_lng = df["longitude"].mean()
```

---

## 📂 Project Structure

```
BI_Cash-Handling-Project/
│
├── scrape_gmaps.py        # Main scraper script (Playwright + Stealth)
├── requirements.txt       # Python dependencies
├── kalsel_cash_nodes.csv  # Output: extracted location records (Kalimantan Selatan)
├── diy_cash_nodes.csv     # Legacy output (DIY/Yogyakarta, previous run)
├── assets/
│   └── banner.png         # Repository banner image
└── README.md              # This documentation
```

### `scrape_gmaps.py` — Module Breakdown

| Section | Lines | Description |
|:--------|:-----:|:------------|
| Configuration | 31–69 | Constants for queries, geo-bounds, delays, output |
| Helper Functions | 80–143 | `human_delay()`, `extract_coords_from_url()`, `build_search_url()` |
| Coordinate Extraction | 146–209 | `extract_coordinates()` — 5-strategy cascade |
| Scroll Logic | 212–267 | `scroll_results_feed()` — infinite scroll handler |
| Query Processor | 270–426 | `scrape_query()` — orchestrates search → scroll → extract |
| Main Orchestrator | 429–553 | `main()` — runs all queries, cleans data, exports CSV |

---

## ⚠ Limitations & Disclaimer

### Technical Limitations

- **Google Maps DOM volatility:** CSS selectors (e.g., `h1.DUwDvf`, `span.DkEaL`) may break if Google updates their front-end. The script includes fallback selectors for resilience.
- **Result cap:** Google Maps search typically returns a maximum of ~100 results per query. Locations beyond this threshold will not be captured.
- **Category accuracy:** Google Maps category labels are community-contributed and may not always reflect the actual business type (e.g., a BRILink agent may be categorized differently).
- **Headed mode required:** The browser must run in visible/headed mode. Headless mode triggers aggressive anti-bot detection on Google Maps.

### Legal Disclaimer

> ⚠️ **This tool is intended for academic and research purposes only.** Automated scraping of Google Maps may violate Google's Terms of Service. For production or commercial use, consider the [Google Places API](https://developers.google.com/maps/documentation/places/web-service/overview), which provides structured, reliable data through official channels.

---

<p align="center">
  <sub>Built for <strong>Bank Indonesia Cash Handling Research</strong> — Kalimantan Selatan, 2026</sub>
</p>
