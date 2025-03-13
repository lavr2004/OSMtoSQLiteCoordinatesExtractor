import osmium
import sqlite3
import os
import datetime
import sys
import requests
import tqdm
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import logging

# base settings
ROOT_FOLDERPATH = os.getcwd()
SOURCE_FOLDERPATH = os.path.join(ROOT_FOLDERPATH, "source")
RESULTS_FOLDERPATH = os.path.join(ROOT_FOLDERPATH, "results")
LOG_FILEPATH = os.path.join(RESULTS_FOLDERPATH, f"osm_processor_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

if not os.path.exists(SOURCE_FOLDERPATH):
    os.makedirs(SOURCE_FOLDERPATH)
if not os.path.exists(RESULTS_FOLDERPATH):
    os.makedirs(RESULTS_FOLDERPATH)

# logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILEPATH),
        logging.StreamHandler(sys.stdout)
    ]
)

def download_file(url, filepath):
    """Download file with progress display"""
    logging.info(f"OK - Downloading {url} to {filepath}...")
    response = requests.get(url, stream=True)
    total_size = int(response.headers.get('content-length', 0))
    block_size = 1024  # 1 KB
    with tqdm.tqdm(total=total_size, unit='B', unit_scale=True, desc="Download") as pbar:
        with open(filepath, 'wb') as f:
            for data in response.iter_content(block_size):
                pbar.update(len(data))
                f.write(data)
    logging.info(f"OK - Download completed: {filepath}")

def get_pbf_links(country):
    """Get list of .pbf files for a country from Geofabrik"""
    base_url = f"https://download.geofabrik.de/europe/{country.lower()}/"
    response = requests.get(base_url)
    if response.status_code != 200:
        logging.error(f"ER - Failed to access {base_url}. Check country name.")
        sys.exit(1)
    soup = BeautifulSoup(response.text, 'html.parser')
    pbf_links = []
    for link in soup.find_all('a', href=True):
        href = link['href']
        if href.endswith('-latest.osm.pbf'):
            pbf_links.append(f"{base_url}{href}")
    return pbf_links

def parse_country_and_region_from_url(url):
    """Extract country and region from URL"""
    parts = urlparse(url).path.split('/')
    if len(parts) >= 4 and parts[1] == 'europe':
        country = parts[2]
        region = parts[3].replace('-latest.osm.pbf', '')
        return country, region
    logging.error(f"ER - Could not parse country and region from URL: {url}")
    sys.exit(1)

def select_pbf_url():
    """Select URL from arguments or interactively"""
    if len(sys.argv) == 2:
        arg = sys.argv[1]
        if arg.endswith('-latest.osm.pbf'):  # Direct URL
            return arg
        else:  # Country specified
            country = arg
            pbf_links = get_pbf_links(country)
            if not pbf_links:
                logging.error(f"ER - No .pbf files found for {country}.")
                sys.exit(1)
            logging.info(f"OK - Available .pbf files for {country}:")
            for i, link in enumerate(pbf_links, 1):
                logging.info(f"OK - {i}) {link}")
            choice = input("\nEnter link number or full URL: ").strip()
            if choice.isdigit():
                idx = int(choice) - 1
                if 0 <= idx < len(pbf_links):
                    return pbf_links[idx]
                else:
                    logging.error("ER - Invalid number.")
                    sys.exit(1)
            else:
                if choice.startswith('http') and choice.endswith('-latest.osm.pbf'):
                    return choice
                else:
                    logging.error("ER - Invalid URL.")
                    sys.exit(1)
    else:
        logging.error("ER - Use: python script.py <country> or python script.py <URL>")
        sys.exit(1)

class AddressCollector(osmium.SimpleHandler):
    def __init__(self, specific_locality=None):
        super().__init__()
        self.node_refs = set()
        self.addresses = {}
        self.specific_locality = specific_locality
        self.total_ways = 0
        self.processed_ways = 0

    def way(self, w):
        self.total_ways += 1
        if 'addr:street' in w.tags and 'addr:housenumber' in w.tags and 'addr:city' in w.tags:
            locality = w.tags['addr:city']
            if self.specific_locality and locality != self.specific_locality:
                return
            street = w.tags['addr:street']
            housenumber = w.tags['addr:housenumber']
            node_refs = [n.ref for n in w.nodes]
            self.node_refs.update(node_refs)
            if locality not in self.addresses:
                self.addresses[locality] = []
            self.addresses[locality].append((street, housenumber, node_refs))
        self.processed_ways += 1
        if self.processed_ways % 10000 == 0:
            logging.info(f"OK - Progress processing ways: {self.processed_ways}/{self.total_ways}")

class NodeCollector(osmium.SimpleHandler):
    def __init__(self, node_refs, nodes_dict):
        super().__init__()
        self.node_refs = node_refs
        self.nodes_dict = nodes_dict
        self.total_nodes = len(node_refs)
        self.processed_nodes = 0

    def node(self, n):
        if n.id in self.node_refs and n.location.valid():
            self.nodes_dict[n.id] = (n.location.lat, n.location.lon)
            self.processed_nodes += 1
            if self.processed_nodes % 100000 == 0:
                logging.info(f"OK - Progress collecting nodes: {self.processed_nodes}/{self.total_nodes} ({(self.processed_nodes/self.total_nodes)*100:.2f}%)")

def convert_polish_locality_name_to_restricted_in_filesystem(raw_locality_name):
    """Convert locality name to filesystem-safe format"""
    dtmodifier = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    if not raw_locality_name:
        return f"noname_{dtmodifier}"
    output_locality_name = ""
    for c in raw_locality_name.lower():
        if c == 'ą':
            c = 'a'
        elif c == ' ':
            c = '_'
        elif c == 'ć':
            c = 'c'
        elif c == 'ę':
            c = 'e'
        elif c == 'ł':
            c = 'l'
        elif c == 'ó':
            c = 'o'
        elif c == 'ś':
            c = 's'
        elif c in 'źż':
            c = 'z'
        elif c not in "aąbcdefghijklmnopqrstuvwxyz_":
            continue
        output_locality_name += c
    return output_locality_name or f"noname_{dtmodifier}"

def create_inventory_db(output_dir, addresses, level='region'):
    """Create inventory database with bounding box data"""
    inventory_db_path = os.path.join(output_dir, "inventory.sqlite")
    conn = sqlite3.connect(inventory_db_path)
    cursor = conn.cursor()

    if level == 'region':
        table_name = 'localities'
        columns = 'locality_name TEXT, db_path TEXT, min_lat REAL, max_lat REAL, min_lon REAL, max_lon REAL'
        data_key = 'locality_name'
    elif level == 'country':
        table_name = 'regions'
        columns = 'region_name TEXT, region_dir TEXT, min_lat REAL, max_lat REAL, min_lon REAL, max_lon REAL'
        data_key = 'region_name'
    else:  # global
        table_name = 'countries'
        columns = 'country_name TEXT, country_dir TEXT, min_lat REAL, max_lat REAL, min_lon REAL, max_lon REAL'
        data_key = 'country_name'

    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            {columns}
        )
    ''')

    for name, data in addresses.items():
        if level == 'region':
            locality_filename = convert_polish_locality_name_to_restricted_in_filesystem(name)
            db_path = os.path.join(output_dir, "localities", f"{locality_filename}.sqlite")
            rel_db_path = os.path.relpath(db_path, ROOT_FOLDERPATH)
            try:
                locality_conn = sqlite3.connect(db_path)
                locality_cursor = locality_conn.cursor()
                locality_cursor.execute('SELECT MIN(latitude), MAX(latitude), MIN(longitude), MAX(longitude) FROM addresses')
                result = locality_cursor.fetchone()
                min_lat, max_lat, min_lon, max_lon = result if result else (None, None, None, None)
                locality_conn.close()
            except Exception as e:
                logging.error(f"ER - Error processing {db_path}: {e}")
                continue
        else:  # country or global
            rel_db_path = data['rel_dir']
            min_lat, max_lat, min_lon, max_lon = data['bounds']

        if min_lat is not None:
            cursor.execute(
                f'INSERT INTO {table_name} ({data_key}, {"db_path" if level == "region" else data_key + "_dir"}, min_lat, max_lat, min_lon, max_lon) VALUES (?, ?, ?, ?, ?, ?)',
                (name, rel_db_path, min_lat, max_lat, min_lon, max_lon)
            )

    conn.commit()
    conn.close()
    logging.info(f"OK - Created inventory database: {inventory_db_path} ({os.path.getsize(inventory_db_path) // 1024} KB)")

def process_addresses_to_db(addresses, nodes_dict, output_dir):
    localities_dir = os.path.join(output_dir, "localities")
    if not os.path.exists(localities_dir):
        os.makedirs(localities_dir)

    for locality, locality_addresses in addresses.items():
        locality_filename = convert_polish_locality_name_to_restricted_in_filesystem(locality)
        db_path = os.path.join(localities_dir, f"{locality_filename}.sqlite")
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS addresses (
                    street TEXT,
                    housenumber TEXT,
                    latitude REAL,
                    longitude REAL
                )
            ''')
            for street, housenumber, node_refs in locality_addresses:
                valid_nodes = [nodes_dict.get(ref) for ref in node_refs if ref in nodes_dict]
                if valid_nodes:
                    lat = sum(coord[0] for coord in valid_nodes) / len(valid_nodes)
                    lon = sum(coord[1] for coord in valid_nodes) / len(valid_nodes)
                    cursor.execute(
                        'INSERT INTO addresses (street, housenumber, latitude, longitude) VALUES (?, ?, ?, ?)',
                        (street, housenumber, lat, lon)
                    )
            conn.commit()
            conn.close()
            logging.info(f"OK - Created database for {locality}: {db_path} ({os.path.getsize(db_path) // 1024} KB)")
        except Exception as e:
            logging.error(f"ER - Error creating database {db_path}: {e}")
            sys.exit(1)

def update_country_inventory(country_dir, region, region_bounds):
    """Update country inventory"""
    inventory_db_path = os.path.join(country_dir, "inventory.sqlite")
    conn = sqlite3.connect(inventory_db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS regions (
            region_name TEXT,
            region_dir TEXT,
            min_lat REAL,
            max_lat REAL,
            min_lon REAL,
            max_lon REAL
        )
    ''')
    rel_region_dir = os.path.relpath(os.path.join(country_dir, region), ROOT_FOLDERPATH)
    cursor.execute(
        'INSERT OR REPLACE INTO regions (region_name, region_dir, min_lat, max_lat, min_lon, max_lon) VALUES (?, ?, ?, ?, ?, ?)',
        (region, rel_region_dir, *region_bounds)
    )
    conn.commit()
    conn.close()
    logging.info(f"OK - Updated country inventory: {inventory_db_path}")

def update_global_inventory(country, country_dir, country_bounds):
    """Update global inventory"""
    inventory_db_path = os.path.join(RESULTS_FOLDERPATH, "inventory.sqlite")
    conn = sqlite3.connect(inventory_db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS countries (
            country_name TEXT,
            country_dir TEXT,
            min_lat REAL,
            max_lat REAL,
            min_lon REAL,
            max_lon REAL
        )
    ''')
    rel_country_dir = os.path.relpath(country_dir, ROOT_FOLDERPATH)
    cursor.execute(
        'INSERT OR REPLACE INTO countries (country_name, country_dir, min_lat, max_lat, min_lon, max_lon) VALUES (?, ?, ?, ?, ?, ?)',
        (country, rel_country_dir, *country_bounds)
    )
    conn.commit()
    conn.close()
    logging.info(f"OK - Updated global inventory: {inventory_db_path}")

if __name__ == "__main__":
    # Select URL
    pbf_url = select_pbf_url()
    pbf_filename = os.path.basename(urlparse(pbf_url).path)
    region_name = pbf_filename.replace("-latest.osm.pbf", "")
    country, region = parse_country_and_region_from_url(pbf_url)

    # Form paths with country
    country_dir = os.path.join(RESULTS_FOLDERPATH, country)
    output_dir = os.path.join(country_dir, region)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    source_filepath = os.path.join(SOURCE_FOLDERPATH, pbf_filename)

    # Download file if it doesn't exist
    if not os.path.exists(source_filepath):
        download_file(pbf_url, source_filepath)
    else:
        logging.info(f"OK - File {source_filepath} already exists, skipping download.")

    # First pass: collect addresses
    logging.info("OK - First pass: collecting addresses...")
    collector = AddressCollector()
    collector.apply_file(source_filepath)
    logging.info(f"OK - Collected addresses: {sum(len(v) for v in collector.addresses.values())} for {len(collector.addresses)} localities")

    # Second pass: collect node coordinates
    logging.info("OK - Second pass: collecting node coordinates...")
    nodes_dict = {}
    node_collector = NodeCollector(collector.node_refs, nodes_dict)
    node_collector.apply_file(source_filepath)
    logging.info(f"OK - Collected node coordinates: {len(nodes_dict)}")

    # Write to locality databases
    logging.info("OK - Writing data to locality databases...")
    process_addresses_to_db(collector.addresses, nodes_dict, output_dir)

    # Create region inventory
    logging.info("OK - Creating region inventory...")
    create_inventory_db(output_dir, collector.addresses, level='region')

    # Calculate region bounding box
    region_inventory_path = os.path.join(output_dir, "inventory.sqlite")
    conn = sqlite3.connect(region_inventory_path)
    cursor = conn.cursor()
    cursor.execute('SELECT MIN(min_lat), MAX(max_lat), MIN(min_lon), MAX(max_lon) FROM localities')
    region_bounds = cursor.fetchone()
    conn.close()

    # Update country inventory
    if region_bounds[0] is not None:  # If there is data
        logging.info("OK - Updating country inventory...")
        update_country_inventory(country_dir, region, region_bounds)

        # Calculate country bounding box
        country_inventory_path = os.path.join(country_dir, "inventory.sqlite")
        conn = sqlite3.connect(country_inventory_path)
        cursor = conn.cursor()
        cursor.execute('SELECT MIN(min_lat), MAX(max_lat), MIN(min_lon), MAX(max_lon) FROM regions')
        country_bounds = cursor.fetchone()
        conn.close()

        # Update global inventory
        if country_bounds[0] is not None:
            logging.info("OK - Updating global inventory...")
            update_global_inventory(country, country_dir, country_bounds)

    logging.info("OK - Processing completed!")