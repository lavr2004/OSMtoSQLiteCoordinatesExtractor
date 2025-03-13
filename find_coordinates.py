import sqlite3
import sys
import os
import logging
import datetime

# Настройка базовых путей
ROOT_FOLDERPATH = os.getcwd()
RESULTS_FOLDERPATH = os.path.join(ROOT_FOLDERPATH, "results")
LOG_FILEPATH = os.path.join(RESULTS_FOLDERPATH, f"coordinates_finder_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILEPATH),
        logging.StreamHandler(sys.stdout)
    ]
)

def parse_address(address_str):
    """Parse address string into components"""
    try:
        parts = [part.strip() for part in address_str.split(',')]
        if len(parts) != 4:
            raise ValueError
        country, locality, street, housenumber = parts
        return country.lower(), locality, street, housenumber
    except ValueError:
        logging.error("ER - Invalid address format. Use: 'Country, Locality, Street, Housenumber'")
        sys.exit(1)

def find_country_dir(country):
    """Find country directory in global inventory"""
    global_inventory_path = os.path.join(RESULTS_FOLDERPATH, "inventory.sqlite")
    if not os.path.exists(global_inventory_path):
        logging.error("ER - Global inventory database not found.")
        return None

    conn = sqlite3.connect(global_inventory_path)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT country_dir 
        FROM countries 
        WHERE lower(country_name) = ?
    ''', (country,))
    result = cursor.fetchone()
    conn.close()

    if result:
        country_dir = os.path.join(ROOT_FOLDERPATH, result[0])
        logging.info(f"OK - Found country directory: {country_dir}")
        return country_dir
    else:
        logging.error(f"ER - Country '{country}' not found in global inventory.")
        return None

def find_locality_db(country_dir, locality):
    """Find locality database across all regions in the country"""
    if not os.path.exists(country_dir):
        logging.error(f"ER - Country directory {country_dir} does not exist.")
        return None

    country_inventory_path = os.path.join(country_dir, "inventory.sqlite")
    if not os.path.exists(country_inventory_path):
        logging.error(f"ER - Country inventory database not found at {country_inventory_path}.")
        return None

    conn = sqlite3.connect(country_inventory_path)
    cursor = conn.cursor()
    cursor.execute('SELECT region_dir FROM regions')
    regions = [os.path.join(ROOT_FOLDERPATH, row[0]) for row in cursor.fetchall()]
    conn.close()

    for region_dir in regions:
        region_inventory_path = os.path.join(region_dir, "inventory.sqlite")
        if not os.path.exists(region_inventory_path):
            logging.warning(f"ER - Region inventory not found at {region_inventory_path}, skipping.")
            continue

        conn = sqlite3.connect(region_inventory_path)
        cursor = conn.cursor()
        cursor.execute('''
            SELECT db_path 
            FROM localities 
            WHERE locality_name = ?
        ''', (locality,))
        result = cursor.fetchone()
        conn.close()

        if result:
            locality_db_path = os.path.join(ROOT_FOLDERPATH, result[0])
            logging.info(f"OK - Found locality database: {locality_db_path}")
            return locality_db_path

    logging.error(f"ER - Locality '{locality}' not found in any region of {country_dir}.")
    return None

def find_coordinates(locality_db_path, street, housenumber):
    """Find coordinates for the given street and housenumber in locality database"""
    if not os.path.exists(locality_db_path):
        logging.error(f"ER - Locality database not found at {locality_db_path}.")
        return None

    conn = sqlite3.connect(locality_db_path)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT latitude, longitude 
        FROM addresses 
        WHERE street = ? AND housenumber = ?
    ''', (street, housenumber))
    result = cursor.fetchone()
    conn.close()

    if result:
        lat, lon = result
        logging.info(f"OK - Found coordinates: ({lat}, {lon}) for {street} {housenumber}")
        return lat, lon
    else:
        logging.error(f"ER - Address '{street} {housenumber}' not found in {locality_db_path}.")
        return None

def get_coordinates(address_str):
    """Get coordinates for the given address"""
    country, locality, street, housenumber = parse_address(address_str)

    # Step 1: Find country directory
    country_dir = find_country_dir(country)
    if not country_dir:
        return None, "ER - Country not found in global inventory."

    # Step 2: Find locality database
    locality_db_path = find_locality_db(country_dir, locality)
    if not locality_db_path:
        return None, "ER - Locality not found in country inventory."

    # Step 3: Find coordinates
    coordinates = find_coordinates(locality_db_path, street, housenumber)
    if not coordinates:
        return None, "ER - Coordinates not found for given address."

    lat, lon = coordinates
    return {
        "country": country,
        "locality": locality,
        "street": street,
        "housenumber": housenumber,
        "latitude": lat,
        "longitude": lon
    }, "OK - Coordinates found."

if __name__ == "__main__":
    if len(sys.argv) != 2:
        logging.error("ER - Usage: python find_coordinates.py 'Country, Locality, Street, Housenumber'")
        sys.exit(1)

    address_str = sys.argv[1]
    logging.info(f"OK - Searching coordinates for address: {address_str}")
    result, msg = get_coordinates(address_str)

    logging.info(msg)
    if result:
        logging.info(f"OK - Country: {result['country']}")
        logging.info(f"OK - Locality: {result['locality']}")
        logging.info(f"OK - Street: {result['street']}")
        logging.info(f"OK - House Number: {result['housenumber']}")
        logging.info(f"OK - Coordinates: ({result['latitude']}, {result['longitude']})")
    else:
        logging.error("ER - Could not determine coordinates for given address.")