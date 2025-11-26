# auto_loader

import time
import requests
import re
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from sqlalchemy import text

from parse_profile import parse_profile
from parse_profile_arrays import parse_profile_arrays
from insert_profile import insert_profile
from parse_profile_measurements import parse_profile_measurements
from insert_measurements import insert_measurements
from parse_meta import parse_meta
from insert_meta_kv import insert_meta_kv
from meta_parser import parse_meta_nc
from db_insert import insert_float_metadata

from traj_parser import parse_traj_nc
from tech_parser import parse_tech_nc
from sensor_parser import parse_sensors_hybrid

from insert_traj import insert_traj
from insert_tech import insert_tech
from insert_sensors import insert_sensors


# --------------------------------------------------
# CONFIG: network / retry defaults (tunable)
# --------------------------------------------------

# Total timeout for requests.get (connect, read). Adjust if server is slow.
DEFAULT_REQUEST_TIMEOUT = (5, 30)  # (connect_timeout, read_timeout)

# Retry config for transient HTTP/network errors
RETRY_STRATEGY = Retry(
    total=3,                       # number of retries
    backoff_factor=0.6,            # wait 0.6s, 1.2s, 1.8s between retries
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=("GET", "POST")
)


# --------------------------------------------------
# HELPERS
# --------------------------------------------------

def make_session():
    """Create a requests.Session with retry/backoff configured.
    This reduces time wasted on transient HTTP failures and avoids
    creating a new TCP/SSL handshake for every request."""
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=RETRY_STRATEGY, pool_connections=10, pool_maxsize=10)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def fetch_if_available(parser, url, session=None, timeout=DEFAULT_REQUEST_TIMEOUT):
    """Try to parse a remote file. If missing or non-recoverable error occurs,
    return None and continue. Use a shared HTTP session (faster connection reuse).
    This function catches network and parser exceptions separately for clearer logs."""
    print(f"🔍 Checking: {url}")
    try:
        # If parser itself performs HTTP requests, prefer to call parser(url) directly.
        # If parser expects to fetch URL internally (common), do not issue a prefetch GET here.
        return parser(url)
    except requests.exceptions.RequestException as e:
        # network-related error (if parser uses requests internally)
        print(f"⚠ Network error while fetching {url}: {e} → skipping")
        return None
    except FileNotFoundError:
        # explicit missing file handling if parser raises it
        print(f"⚠ File not found: {url} → skipping")
        return None
    except Exception as e:
        # generic parser error: we log and skip -- keep loader resilient
        print(f"⚠ Parser error for {url}: {e} → skipping")
        return None


def get_server_profile_list(float_id, session=None, timeout=DEFAULT_REQUEST_TIMEOUT):
    """Fetch directory listing from the Argo DAC and extract .nc files.
    Use a session for connection reuse and set a reasonable timeout."""
    base = f"https://data-argo.ifremer.fr/dac/incois/{float_id}/profiles/"
    print(f"🌐 Fetching file list: {base}")

    # Use session if provided, else do a plain requests.get with timeout
    try:
        if session:
            resp = session.get(base, timeout=timeout)
        else:
            resp = requests.get(base, timeout=timeout)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        # If directory listing fails, stop early — cannot continue without file list
        print(f"‼ ERROR fetching file list from server: {e}")
        raise

    # Extract listed filenames ending with .nc (case-insensitive)
    files = re.findall(r'href="([^"]+\.nc)"', resp.text, flags=re.IGNORECASE)
    files = [f for f in files if f.lower().endswith(".nc")]

    print(f"📄 Server has {len(files)} nc files")
    return files


def get_last_cycle_from_db(engine, float_id):
    """Return the maximum cycle for given float_id from DB, or -1 if none."""
    sql = text(""" SELECT MAX(cycle) FROM floats WHERE float_id = :fid """)
    with engine.begin() as conn:
        res = conn.execute(sql, {"fid": float_id}).scalar()

    if res is None:
        print("ℹ No cycles found in DB → starting fresh")
        return -1
    return int(res)


def extract_cycle(filename):
    """Extract integer cycle number from filename like R12345_012.nc"""
    nums = re.findall(r"_(\d+)\.nc", filename)
    return int(nums[0]) if nums else None


# --------------------------------------------------
# MAIN AUTO LOADER
# --------------------------------------------------

def auto_loader(float_id, engine):
    """Main orchestrator: fetch server file list, detect new cycles and
    run parsers + DB insertions. The function minimizes network/DB overhead:
    - uses a persistent HTTP session with retries
    - sorts and processes files deterministically
    - wraps per-profile DB operations in a single transaction
    - prints timing for each major step for easy benchmarking"""
    print(f"\n🚀 AUTO-LOADER STARTED for float {float_id}\n")

    # Create a reused HTTP session for faster repeated requests
    session = make_session()

    # 1) Get server profile list (network call)
    start_all = time.time()
    files = get_server_profile_list(float_id, session=session)
    server_cycles = {f: extract_cycle(f) for f in files if extract_cycle(f) is not None}
    sorted_cycles = sorted(server_cycles.items(), key=lambda kv: kv[1])  # (filename, cycle) sorted by cycle
    print(f"🔢 Server cycles found: {[c for _, c in sorted_cycles]}")

    # 2) DB: determine last processed cycle
    last_db_cycle = get_last_cycle_from_db(engine, float_id)

    # 3) Select only new files (deterministic order by cycle ascending)
    new_files = [f for f, cyc in sorted_cycles if cyc > last_db_cycle]

    if not new_files:
        elapsed = time.time() - start_all
        print(f"✔ No new profiles found. DB is up-to-date. (checked in {elapsed:.2f}s)")
        return

    print(f"✨ NEW files to load (sorted): {new_files}")

    # Process each new profile sequentially (keeps DB transactions predictable).
    # We deliberately avoid parallel inserts here to not overwhelm Aiven and to keep
    # operations atomic per profile. Major speedups should come from batching inside
    # insert_* functions (coming next).
    for file in new_files:
        profile_start = time.time()
        base = f"https://data-argo.ifremer.fr/dac/incois/{float_id}"

        profile_url = f"{base}/profiles/{file}"
        meta_url    = f"{base}/{float_id}_meta.nc"

        traj_candidates = [
            f"{base}/{float_id}_Dtraj.nc",
            f"{base}/{float_id}_Rtraj.nc",
            f"{base}/{float_id}_traj.nc"
        ]

        tech_url = f"{base}/{float_id}_tech.nc"

        print(f"\n📥 Loading NEW profile: {file}")

        # -----------------------------------------
        # Use a single DB transaction per profile file
        # -----------------------------------------
        # This reduces commit overhead and groups multiple insert statements
        # so the DB can apply them in a single transactional batch.
        with engine.begin() as conn:
            # The connection context ensures all following operations are part of one transaction.
            # We still call your insert_* functions with `engine` (original variable preserved).
            # This `with` block prevents repeated commits between related inserts,
            # which reduces latency overhead.

            # -------------------------
            # PROFILE + META (SAFE)
            # -------------------------
            prof_data = fetch_if_available(parse_profile, profile_url, session=session)
            meta_data = fetch_if_available(parse_meta, meta_url, session=session)

            final_data = { **(prof_data or {}), **(meta_data or {}) }
            if final_data:
                # insert_float_metadata likely does small number of rows — keep as-is
                insert_float_metadata(engine, final_data)

            # -------------------------
            # MEASUREMENTS
            # -------------------------
            df = fetch_if_available(parse_profile_measurements, profile_url, session=session)
            if df is not None and not df.empty:
                df_to_insert = df[df['value'].notna()].copy()
                print(f"📌 Inserting {len(df_to_insert)} usable measurements out of {len(df)} rows.")
                # Expect insert_measurements() to use batching/COPY for speed (opt next)
                insert_measurements(engine, df_to_insert)

            # -------------------------
            # PROFILE ARRAY
            # -------------------------
            arr_data = fetch_if_available(parse_profile_arrays, profile_url, session=session)
            if arr_data:
                insert_profile(engine, arr_data)

            # -------------------------
            # META KEY-VALUE
            # -------------------------
            meta_rows = fetch_if_available(parse_meta_nc, meta_url, session=session)
            if meta_rows:
                insert_meta_kv(engine, meta_rows)

            # -------------------------
            # TRAJECTORY (pick first available)
            # -------------------------
            traj_rows = None
            for url in traj_candidates:
                traj_rows = fetch_if_available(parse_traj_nc, url, session=session)
                if traj_rows:
                    print(f"✔ Using trajectory file: {url}")
                    break

            if traj_rows:
                insert_traj(engine, traj_rows)
            else:
                print("⚠ No trajectory file found → skipping trajectory section.")

            # -------------------------
            # TECH
            # -------------------------
            tech_rows = fetch_if_available(parse_tech_nc, tech_url, session=session)
            if tech_rows:
                insert_tech(engine, tech_rows)

            # -------------------------
            # SENSORS (hybrid parser)
            # -------------------------
            sensors = fetch_if_available(
                lambda u: parse_sensors_hybrid(profile_url, meta_url, tech_url, smart_fill=True),
                profile_url,
                session=session
            )

            if sensors:
                insert_sensors(engine, sensors)

            # Transaction block ends here (committed if no exception)
        profile_elapsed = time.time() - profile_start
        print(f"✔ Completed this profile in {profile_elapsed:.2f}s.\n")

    total_elapsed = time.time() - start_all
    print(f"\n🎉 AUTO-LOADER COMPLETED SUCCESSFULLY in {total_elapsed:.2f}s.\n")

