import sqlite3
import sys
import os
import logging
import datetime

# Настройка базовых путей
ROOT_FOLDERPATH = os.getcwd()
RESULTS_FOLDERPATH = os.path.join(ROOT_FOLDERPATH, "results")
LOG_FILEPATH = os.path.join(RESULTS_FOLDERPATH, f"address_finder_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILEPATH),
        logging.StreamHandler(sys.stdout)
    ]
)

def find_country(lat, lon):
    """Find country containing the given coordinates"""
    global_inventory_path = os.path.join(RESULTS_FOLDERPATH, "inventory.sqlite")
    if not os.path.exists(global_inventory_path):
        logging.error("ER - Global inventory database not found.")
        return None

    conn = sqlite3.connect(global_inventory_path)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT country_name, country_dir 
        FROM countries 
        WHERE ? BETWEEN min_lat AND max_lat 
        AND ? BETWEEN min_lon AND max_lon
    ''', (lat, lon))
    result = cursor.fetchone()
    conn.close()

    if result:
        country_name, country_dir = result
        logging.info(f"OK - Found country: {country_name}")
        return country_name, os.path.join(ROOT_FOLDERPATH, country_dir)
    else:
        logging.error("ER - No country found for coordinates ({}, {}).".format(lat, lon))
        return None

def find_region(country_dir, lat, lon):
    """Find region containing the given coordinates"""
    country_inventory_path = os.path.join(country_dir, "inventory.sqlite")
    if not os.path.exists(country_inventory_path):
        logging.error(f"ER - Country inventory database not found at {country_inventory_path}.")
        return None

    conn = sqlite3.connect(country_inventory_path)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT region_name, region_dir 
        FROM regions 
        WHERE ? BETWEEN min_lat AND max_lat 
        AND ? BETWEEN min_lon AND max_lon
    ''', (lat, lon))
    result = cursor.fetchone()
    conn.close()

    if result:
        region_name, region_dir = result
        logging.info(f"OK - Found region: {region_name}")
        return region_name, os.path.join(ROOT_FOLDERPATH, region_dir)
    else:
        logging.error("ER - No region found for coordinates ({}, {}).".format(lat, lon))
        return None

def find_locality(region_dir, lat, lon):
    """Find locality containing the given coordinates"""
    region_inventory_path = os.path.join(region_dir, "inventory.sqlite")
    if not os.path.exists(region_inventory_path):
        logging.error(f"ER - Region inventory database not found at {region_inventory_path}.")
        return None

    conn = sqlite3.connect(region_inventory_path)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT locality_name, db_path 
        FROM localities 
        WHERE ? BETWEEN min_lat AND max_lat 
        AND ? BETWEEN min_lon AND max_lon
    ''', (lat, lon))
    result = cursor.fetchone()
    conn.close()

    if result:
        locality_name, db_path = result
        logging.info(f"OK - Found locality: {locality_name}")
        return locality_name, os.path.join(ROOT_FOLDERPATH, db_path)
    else:
        logging.error("ER - No locality found for coordinates ({}, {}).".format(lat, lon))
        return None

def find_nearest_address(locality_db_path, lat, lon):
    """Find the nearest address in the locality database"""
    if not os.path.exists(locality_db_path):
        logging.error(f"ER - Locality database not found at {locality_db_path}.")
        return None

    conn = sqlite3.connect(locality_db_path)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT street, housenumber, latitude, longitude,
               ((latitude - ?) * (latitude - ?) + (longitude - ?) * (longitude - ?)) AS distance
        FROM addresses
        ORDER BY distance
        LIMIT 1
    ''', (lat, lat, lon, lon))
    result = cursor.fetchone()
    conn.close()

    if result:
        street, housenumber, address_lat, address_lon, distance = result
        logging.info(f"OK - Nearest address found: {street} {housenumber} (lat: {address_lat}, lon: {address_lon})")
        return street, housenumber, address_lat, address_lon
    else:
        logging.error("ER - No addresses found in locality database.")
        return None

def get_address(lat, lon):
    """Get address for given coordinates by traversing the database hierarchy"""
    # Step 1: Find country
    country_result = find_country(lat, lon)
    if not country_result:
        return None, "ER - Could not determine country for given coordinates."
    country_name, country_dir = country_result

    # Step 2: Find region
    region_result = find_region(country_dir, lat, lon)
    if not region_result:
        return None, "ER - Could not determine region for given coordinates."
    region_name, region_dir = region_result

    # Step 3: Find locality
    locality_result = find_locality(region_dir, lat, lon)
    if not locality_result:
        return None, "ER - Could not determine locality for given coordinates (city, town, village, etc)."
    locality_name, locality_db_path = locality_result

    # Step 4: Find nearest address
    address_result = find_nearest_address(locality_db_path, lat, lon)
    if not address_result:
        return None, "ER - Could not determine address for given coordinates."
    street, housenumber, address_lat, address_lon = address_result

    return {
        "country": country_name,
        "region": region_name,
        "locality": locality_name,
        "street": street,
        "housenumber": housenumber,
        "latitude": address_lat,
        "longitude": address_lon
    }, "OK - Address found."

if __name__ == "__main__":
    if len(sys.argv) != 3:
        logging.error("ER - Usage: python find_address.py <latitude> <longitude>")
        sys.exit(1)

    try:
        lat = float(sys.argv[1])
        lon = float(sys.argv[2])
    except ValueError:
        logging.error("ER - Invalid coordinates. Please provide numeric latitude and longitude.")
        sys.exit(1)

    logging.info(f"OK - Searching address for coordinates: ({lat}, {lon})")
    address, msg = get_address(lat, lon)

    logging.info(msg)
    if address:
        logging.info(f"OK - Country: {address['country']}")
        logging.info(f"OK - Region: {address['region']}")
        logging.info(f"OK - Locality: {address['locality']}")
        logging.info(f"OK - Street: {address['street']}")
        logging.info(f"OK - House Number: {address['housenumber']}")
        logging.info(f"OK - Coordinates: ({address['latitude']}, {address['longitude']})")
    else:
        logging.error("ER - Could not determine address for given coordinates.")