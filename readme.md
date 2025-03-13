# OSM Address Processor

![Python](https://img.shields.io/badge/Python-red?logo=python&logoColor=white)
![OpenStreetMap](https://img.shields.io/badge/OpenStreetMap-green?logo=openstreetmap&logoColor=white)
![SQLite](https://img.shields.io/badge/SQLite-blue?logo=SQLite&logoColor=white)

This Python script downloads OpenStreetMap (OSM) `.pbf` files from Geofabrik, extracts address data (streets, house numbers, and coordinates), and organizes them into SQLite databases. It creates a hierarchical structure of inventories for countries, regions, and localities, suitable for offline geolocation applications.

---

## Features
- Downloads `.pbf` files from Geofabrik with progress display.
- Supports interactive selection of `.pbf` files by country or direct URL input.
- Extracts address data (`addr:street`, `addr:housenumber`, `addr:city`) from `.pbf` files.
- Creates SQLite databases for each locality (cities, towns, villages) in `results/<country>/<region>/localities/`.
- Generates inventory databases (`inventory.sqlite`) at three levels:
   - **Global**: `results/inventory.sqlite` (countries).
   - **Country**: `results/<country>/inventory.sqlite` (regions).
   - **Region**: `results/<country>/<region>/inventory.sqlite` (localities).
- Computes bounding boxes (min/max latitude and longitude) for each locality, region, and country.
- Uses relative paths for database references.
- Logs operations to both console and a file in `results/`.

---

## Directory Structure

results/
├── inventory.sqlite              # Global inventory of countries  
├── poland/  
│   ├── inventory.sqlite          # Inventory of regions in Poland  
│   ├── mazowieckie/  
│   │   ├── inventory.sqlite      # Inventory of localities in Mazowieckie  
│   │   └── localities/  
│   │       ├── warszawa.sqlite   # Addresses in Warszawa  
│   │       ├── radom.sqlite      # Addresses in Radom  
│   └── lubuskie/  
│       ├── inventory.sqlite  
│       └── localities/  
│           ├── gorzow_wielkopolski.sqlite  
└── osm_processor_20250312_123456.log  # Log file  

---

## Database Schemas

```ddl
- Locality Database (`<locality>.sqlite`):
   - Table: `addresses`
      - `street` (TEXT): Street name.
      - `housenumber` (TEXT): House number.
      - `latitude` (REAL): Latitude.
      - `longitude` (REAL): Longitude.
```

```ddl
- Inventory Database (`inventory.sqlite`):
   - Region level: Table `localities`
      - `locality_name` (TEXT), `db_path` (TEXT), `min_lat` (REAL), `max_lat` (REAL), `min_lon` (REAL), `max_lon` (REAL)
   - Country level: Table `regions`
      - `region_name` (TEXT), `region_dir` (TEXT), `min_lat` (REAL), `max_lat` (REAL), `min_lon` (REAL), `max_lon` (REAL)
   - Global level: Table `countries`
      - `country_name` (TEXT), `country_dir` (TEXT), `min_lat` (REAL), `max_lat` (REAL), `min_lon` (REAL), `max_lon` (REAL)
```

---

## Requirements
- Python 3.10+
- Libraries listed in `requirements.txt`

---

## Installation
1. Clone this repository or download the script.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

---

## Usage
1. Run with a country name to list available `.pbf` files:
   ```bash
   python main.py poland
   ```
   - Select a number or enter a full URL when prompted.
2. Run with a direct URL:
   ```bash
   python main.py https://download.geofabrik.de/europe/poland/mazowieckie-latest.osm.pbf
   ```
3. The script will:
   - Download the `.pbf` file to `source/` if not already present.
   - Process addresses and create databases in `results/<country>/<region>/localities/`.
   - Update inventory databases at all levels.

---
## Logging
- Logs are saved to `results/osm_processor_<datetime>.log` and displayed in the console.
- Messages are prefixed with "OK - " for success or "ER - " for errors.

---
## Notes
- Locality names are normalized to remove Polish diacritics (e.g., "Żyrardów" → "zyrardow").
- The script uses a two-pass approach to optimize memory usage.
- Bounding boxes enable offline geolocation by checking coordinate containment.