# auto_loader.py

import time
import requests
import re
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from sqlalchemy import text
from concurrent.futures import ThreadPoolExecutor, as_completed

from dataset_cache import CACHE

from parsers.profile import parse_profile
from parsers.profile_arrays import parse_profile_arrays
from database.insert_profile import insert_profile
from parsers.measurements import parse_profile_measurements
from database.insert_measurements import insert_measurements
from parsers.meta import parse_meta
from database.insert_meta_kv import insert_meta_kv
from parsers.meta_legacy import parse_meta_nc
from database.insert_float import insert_float_metadata

from parsers.traj import parse_traj_nc
from parsers.tech import parse_tech_nc
from parsers.sensors import parse_sensors_hybrid

from database.insert_traj import insert_traj
from database.insert_tech import insert_tech
from database.insert_sensors import insert_sensors


# --------------------------------------------------
# CONFIG: network / retry defaults (tunable)
# --------------------------------------------------

DEFAULT_REQUEST_TIMEOUT = (5, 30)

# Ensure data directory exists
DATA_DIR = "dummy/data"
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

def get_float_dir(float_id):
    path = os.path.join(DATA_DIR, str(float_id))
    if not os.path.exists(path):
        os.makedirs(path)
    return path

RETRY_STRATEGY = Retry(
    total=3,
    backoff_factor=0.6,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=("GET", "POST")
)


def make_session():
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=RETRY_STRATEGY, pool_connections=10, pool_maxsize=10)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def download_file(url, float_id):
    filename = url.split("/")[-1]
    float_dir = get_float_dir(float_id)
    filepath = os.path.join(float_dir, filename)
    
    if os.path.exists(filepath):
        return filepath
        
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code == 200:
            with open(filepath, "wb") as f:
                f.write(resp.content)
            return filepath
        else:
            # print(f"⚠ Download failed: {url} (status {resp.status_code})")
            return None
    except Exception as e:
        print(f"❌ Error downloading {url}: {e}")
        return None


def fetch_if_available(parser, url, session=None, timeout=DEFAULT_REQUEST_TIMEOUT):
    # print(f"🔍 Checking: {url}")
    try:
        return parser(url)
    except requests.exceptions.RequestException as e:
        print(f"⚠ Network error while fetching {url}: {e} → skipping")
        return None
    except Exception as e:
        print(f"⚠ Parser error for {url}: {e} → skipping")
        return None


def get_server_profile_list(float_id, session=None, timeout=DEFAULT_REQUEST_TIMEOUT):
    base = f"https://data-argo.ifremer.fr/dac/incois/{float_id}/profiles/"
    # print(f"🌐 Fetching file list: {base}")

    try:
        if session:
            resp = session.get(base, timeout=timeout)
        else:
            resp = requests.get(base, timeout=timeout)
        resp.raise_for_status()
    except Exception as e:
        print(f"‼ ERROR fetching file list from server: {e}")
        raise

    files = re.findall(r'href="([^"]+\.nc)"', resp.text, flags=re.IGNORECASE)
    return files


def extract_cycle(filename):
    nums = re.findall(r"_(\d+)\.nc", filename)
    return int(nums[0]) if nums else None



def get_existing_cycles(engine, float_id):
    """
    Fetch all cycle numbers that already exist in the database for this float.
    Returns a set of integers.
    """
    # We check the 'profiles' table as it is the main source of cycle data
    sql = text("SELECT DISTINCT cycle FROM profiles WHERE float_id = :fid")
    with engine.begin() as conn:
        rows = conn.execute(sql, {"fid": float_id}).fetchall()
    
    return {row[0] for row in rows}


# --------------------------------------------------
# HELPER: Deduplicate files (S > D > R)
# --------------------------------------------------
def filter_best_files(files):
    """
    Filter list of files to keep only the 'best' version for each cycle.
    Priority: SD > SR > BD > BR > D > R
    """
    # Group by cycle
    cycle_map = {}
    for f in files:
        # Extract cycle from filename (e.g., R1902675_001.nc -> 001)
        # Standard format: [Prefix][FloatID]_[Cycle][Suffix].nc
        try:
            parts = f.split("_")
            if len(parts) < 2:
                continue
            
            # Last part starts with cycle number (e.g. "001.nc" or "001D.nc")
            cycle_part = parts[-1].replace(".nc", "")
            # Extract numeric cycle
            cycle_str = "".join(filter(str.isdigit, cycle_part))
            if not cycle_str:
                continue
            cycle = int(cycle_str)
            
            if cycle not in cycle_map:
                cycle_map[cycle] = []
            cycle_map[cycle].append(f)
        except Exception:
            continue

    final_list = []
    
    # Priority order checks
    # We look for prefixes in the filename
    def get_score(fname):
        base = os.path.basename(fname)
        if base.startswith("SD"): return 6
        if base.startswith("SR"): return 5
        if base.startswith("BD"): return 4
        if base.startswith("BR"): return 3
        if base.startswith("D"): return 2
        if base.startswith("R"): return 1
        return 0

    for cycle, file_list in cycle_map.items():
        # Sort by score descending
        file_list.sort(key=get_score, reverse=True)
        # Pick the best one
        best_file = file_list[0]
        final_list.append(best_file)

    return sorted(final_list)


# --------------------------------------------------
# WORKER: Process Single File
# --------------------------------------------------
def process_single_file(file, float_id, engine, base, meta_data):
    """
    Worker function to process a single profile file.
    Only handles Profile and Measurements (Cycle-specific data).
    """
    profile_start = time.time()
    profile_url = base + "/profiles/" + file
    
    # print(f"📥 Loading NEW profile: {file}")

    # Download file
    # We need to pass float_id to download_file now
    # But wait, fetch_if_available takes a parser and url.
    # The parsers usually take a URL or a filename.
    # Our parsers (e.g. parse_profile_arrays) expect a local filepath or URL?
    # Looking at previous code, they take a URL/Path.
    
    # To support caching, we should download first.
    local_file = download_file(profile_url, float_id)
    if not local_file:
        return False

    # 1. Parse Profile (Core)
    # Use parse_profile_arrays to get FULL data (arrays + metadata)
    # parse_profile only returns metadata, causing NULLs in profiles table.
    # Now we pass the LOCAL FILE path
    try:
        prof_data = parse_profile_arrays(local_file)
    except Exception as e:
        print(f"⚠ Parser error for {file}: {e}")
        return False
        
    if not prof_data:
        return False

    # 2. Parse Measurements
    try:
        measure_data = parse_profile_measurements(local_file)
    except Exception:
        measure_data = None

    # --------------------------------------------------
    # DATABASE TRANSACTIONS
    # --------------------------------------------------
    try:
        with engine.begin() as conn:
            # A. Insert Float Metadata (using PRE-LOADED data)
            # We merge profile info (lat/lon/time) with static metadata
            if meta_data and prof_data:
                # Map lat/lon to latitude/longitude if missing (parse_profile_arrays uses short names)
                if "lat" in prof_data and "latitude" not in prof_data:
                    prof_data["latitude"] = prof_data["lat"]
                if "lon" in prof_data and "longitude" not in prof_data:
                    prof_data["longitude"] = prof_data["lon"]

                # Combine dicts
                full_meta = {**meta_data, **prof_data}
                # print(f"   Inserting float metadata for {float_id} cycle {cycle}")
                try:
                    insert_float_metadata(conn, full_meta)
                    # print("   ✔ Inserted float metadata")
                except Exception as e:
                    print(f"   ❌ Failed to insert float metadata: {e}")

            # B. Insert Profile
            if prof_data:
                insert_profile(conn, prof_data)

            # C. Insert Measurements
            if measure_data is not None:
                insert_measurements(conn, measure_data)
            
        # print(f"✔ Completed {file} in {time.time() - profile_start:.2f}s")
        return True

    except Exception as e:
        print(f"❌ FAILED to process {file}: {e}")
        return False


# --------------------------------------------------
# --------------------------------------------------
# MAIN LOADER
# --------------------------------------------------
def auto_loader(float_id, engine):
    start_all = time.time()
    
    # List of known DACs (Data Assembly Centers)
    DACS = ["incois", "aoml", "coriolis", "csiro", "jma", "bodc", "kma", "meds", "kordi"]
    
    base = None
    found_dac = None
    
    # Try to find which DAC holds this float
    for dac in DACS:
        test_url = f"https://data-argo.ifremer.fr/dac/{dac}/{float_id}"
        # We check if the directory exists by trying to fetch the profile list page
        # or just the base page. A HEAD request is faster.
        try:
            # Using HEAD to check existence quickly
            resp = requests.head(test_url + "/", timeout=5)
            if resp.status_code == 200:
                base = test_url
                found_dac = dac
                # print(f"🌍 Found float {float_id} in DAC: {dac}")
                break
        except Exception:
            continue
            
    if not base:
        print(f"❌ Float {float_id} not found in any known DAC.")
        return

    # print(f"\n⚙ Running auto_loader for {float_id}...")

    # 1. Get list of files
    try:
        resp = requests.get(base + "/profiles/")
        resp.raise_for_status()
        # Simple parsing of hrefs
        all_files = [
            line.split('href="')[1].split('"')[0] 
            for line in resp.text.splitlines() 
            if 'href="' in line and ".nc" in line
        ]
    except Exception as e:
        print(f"❌ Failed to list profiles: {e}")
        return

    # 2. Filter Duplicates (Keep Best Version)
    new_files = filter_best_files(all_files)
    
    # --------------------------------------------------
    # OPTIMIZATION: Skip Existing Cycles
    # --------------------------------------------------
    existing_cycles = get_existing_cycles(engine, float_id)
    files_to_process = []
    skipped_count = 0

    for f in new_files:
        cycle = extract_cycle(f)
        if cycle is not None and cycle in existing_cycles:
            skipped_count += 1
            continue
        files_to_process.append(f)

    if skipped_count > 0:
        print(f"⏩ Skipping {skipped_count} existing cycles (Already in DB).")

    if len(all_files) > len(new_files):
        print(f"📊 Found {len(all_files)} files, filtered to {len(new_files)} unique cycles.")
    else:
        print(f"📊 Found {len(new_files)} files.")
    
    print(f"🚀 Processing {len(files_to_process)} new files.")

    if not files_to_process:
        print(f"✅ No new files to process. DONE in {time.time() - start_all:.2f}s")
        return

    # 3. PRE-LOAD METADATA (ONCE)
    print("📦 Pre-loading metadata...")
    meta_url = f"{base}/{float_id}_meta.nc"
    meta_file = download_file(meta_url, float_id)
    
    meta_data = None
    if meta_file:
        try:
            meta_data = parse_meta(meta_file)
        except Exception:
            pass
    
    # Parse Meta KV (Legacy/Detailed)
    meta_rows = []
    if meta_file:
        try:
            meta_nc_data = parse_meta_nc(meta_file)
            if meta_nc_data:
                meta_rows = meta_nc_data
        except Exception:
            pass
    
    print(f"📦 Meta KV rows found: {len(meta_rows)}")

    # 4. Process Float-Level Data (ONCE)
    # --------------------------------------------------
    print("📦 Processing Float-Level Data (Meta, Tech, Traj)...")
    
    # Define helper functions for parallel fetching
    def fetch_tech():
        tech_url = f"{base}/{float_id}_tech.nc"
        t_file = download_file(tech_url, float_id)
        if t_file:
            return parse_tech_nc(t_file)
        return None

    def fetch_traj():
        for suffix in ["_Dtraj.nc", "_Rtraj.nc", "_traj.nc"]:
            t_url = f"{base}/{float_id}{suffix}"
            t_file = download_file(t_url, float_id)
            if t_file:
                rows = parse_traj_nc(t_file)
                if rows:
                    print(f"✔ Found trajectory file: {t_url}")
                    return rows
        return None

    try:
        # Fetch in parallel
        with ThreadPoolExecutor(max_workers=3) as meta_executor:
            future_tech = meta_executor.submit(fetch_tech)
            future_traj = meta_executor.submit(fetch_traj)
            
            tech_rows = future_tech.result()
            traj_rows = future_traj.result()

        if not traj_rows:
            print(f"⚠ Trajectory file not found for {float_id} (checked Dtraj/Rtraj/traj)")

        with engine.begin() as conn:
            # Insert Meta KV
            if meta_rows:
                insert_meta_kv(conn, meta_rows)
            
            # Insert Tech
            if tech_rows:
                insert_tech(conn, float_id, tech_rows)

            # Insert Traj
            if traj_rows:
                insert_traj(conn, traj_rows)
                
    except Exception as e:
        print(f"❌ Failed to process float-level data: {e}")

    # 5. Parallel Processing (Profiles Only)
    # We use fewer workers to avoid locking issues since we are efficient now
    MAX_WORKERS = 10 
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_file = {
            executor.submit(process_single_file, file, float_id, engine, base, meta_data): file
            for file in files_to_process
        }

        for future in as_completed(future_to_file):
            file = future_to_file[future]
            try:
                future.result()
            except KeyboardInterrupt:
                print("\n🛑 Stopped by User")
                executor.shutdown(wait=False)
                return
            except Exception as exc:
                print(f"‼ Generated an exception: {exc}")

    print(f"\n🎉 DONE in {time.time() - start_all:.2f}s")
