#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
OceanIQ — FINAL STABLE v3
- Robust parsing (handles None, NaN, bytes, masked arrays)
- Chunked bulk inserts for measurements
- Safe re-run and resume
- Retry downloads, .part temp file writes
- SQLAlchemy param style consistent (:name)
"""

import os
import sys
import json
import time
import logging
import requests
import numpy as np
from netCDF4 import Dataset
from tqdm import tqdm
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

load_dotenv()

# ------------ CONFIG ------------
DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:ocean@localhost:5432/dbphase3")
DAC_ROOT = os.getenv("DAC_ROOT", "https://data-argo.ifremer.fr/dac")
DEFAULT_DAC = os.getenv("DEFAULT_DAC", "aoml")
BASE_DIR = os.getenv("ARGO_BASE_DIR", "./argo_data")
DOWNLOAD_TIMEOUT = int(os.getenv("DOWNLOAD_TIMEOUT", "30"))
DOWNLOAD_RETRIES = int(os.getenv("DOWNLOAD_RETRIES", "3"))
MEASUREMENT_CHUNK = int(os.getenv("MEASUREMENT_CHUNK", "400"))

engine = create_engine(DB_URL)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("ocean_ingest_v3")

# ------------ Utilities ------------
def ensure_dir(p):
    os.makedirs(p, exist_ok=True)

def is_number(x):
    try:
        if x is None:
            return False
        if isinstance(x, (str, bytes, bytearray)):
            # reject non-numeric strings
            float(x)
            return True
        # numpy types handled
        float(x)
        return True
    except Exception:
        return False

def to_number_or_none(x):
    if x is None:
        return None
    # handle bytes
    if isinstance(x, (bytes, bytearray)):
        try:
            s = x.decode("utf-8", errors="ignore")
            return float(s)
        except Exception:
            return None
    # numpy scalar
    if isinstance(x, np.generic):
        try:
            return float(np.array(x).item())
        except:
            return None
    try:
        return float(x)
    except:
        return None

# ------------ Download with retries ------------
def download_if_needed(url, dest, timeout=DOWNLOAD_TIMEOUT, retries=DOWNLOAD_RETRIES):
    if os.path.exists(dest):
        logger.debug(f"Exists: {dest}")
        return True
    ensure_dir(os.path.dirname(dest))
    for attempt in range(1, retries+1):
        try:
            logger.info(f"Downloading: {url} (attempt {attempt})")
            r = requests.get(url, stream=True, timeout=timeout)
            if r.status_code != 200:
                logger.warning(f"HTTP {r.status_code} for {url}")
                time.sleep(1)
                continue
            tmp = dest + ".part"
            with open(tmp, "wb") as fh:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        fh.write(chunk)
            os.replace(tmp, dest)
            logger.info(f"Saved: {dest}")
            return True
        except KeyboardInterrupt:
            raise
        except Exception as e:
            logger.warning(f"Download error ({attempt}): {e}")
            time.sleep(1)
    logger.error(f"Failed download after {retries} attempts: {url}")
    return False

# ------------ Deep clean meta ------------
def deep_clean(obj):
    # bytes → str
    if isinstance(obj, (bytes, bytearray)):
        return obj.decode("utf-8", errors="ignore")
    # numpy masked array
    try:
        import numpy.ma as ma
        if isinstance(obj, ma.MaskedArray):
            obj = obj.filled(np.nan)
    except Exception:
        pass
    # numpy scalar
    if isinstance(obj, np.generic):
        try:
            return obj.item()
        except:
            try:
                return float(obj)
            except:
                return str(obj)
    # numpy array -> tolist then clean
    if isinstance(obj, np.ndarray):
        try:
            lst = obj.tolist()
        except:
            lst = [x for x in obj]
        return deep_clean(lst)
    # list/tuple
    if isinstance(obj, list):
        return [deep_clean(x) for x in obj]
    if isinstance(obj, tuple):
        return tuple(deep_clean(x) for x in obj)
    # dict
    if isinstance(obj, dict):
        return { (deep_clean(k) if not isinstance(k, str) else k): deep_clean(v) for k,v in obj.items()}
    return obj

# ------------ Safe meta reader ------------
def read_meta_file(path):
    meta = {}
    try:
        ds = Dataset(path)
    except Exception as e:
        logger.warning(f"Cannot open meta: {path}: {e}")
        return {}
    for name in ds.variables:
        try:
            v = ds.variables[name][...]
        except Exception:
            try:
                v = ds.variables[name][:]
            except Exception:
                v = None
        # masked -> filled
        try:
            import numpy.ma as ma
            if isinstance(v, ma.MaskedArray):
                v = v.filled(np.nan)
        except Exception:
            pass
        meta[name] = v
    ds.close()
    return deep_clean(meta)

# ------------ Profile parser (robust) ------------
def safe_get_var(ds, names):
    for n in names:
        if n in ds.variables:
            try:
                val = ds.variables[n][...]
                try:
                    import numpy.ma as ma
                    if isinstance(val, ma.MaskedArray):
                        val = val.filled(np.nan)
                except:
                    pass
                return val
            except:
                try:
                    return ds.variables[n][:]
                except:
                    continue
    return None

def parse_profile_file(path):
    """
    Return list of parsed profiles (each dict with keys below).
    Handles files which may contain multiple profiles or scalars.
    """

    try:
        ds = Dataset(path)
    except Exception as e:
        logger.error(f"Open error {path}: {e}")
        return []

    # common names
    lat_v = safe_get_var(ds, ["LATITUDE", "latitude"])
    lon_v = safe_get_var(ds, ["LONGITUDE", "longitude"])
    juld_v = safe_get_var(ds, ["JULD", "juld", "JULD_LOCATION"])
    cycle_v = safe_get_var(ds, ["CYCLE_NUMBER", "cycle_number"])
    profnum_v = safe_get_var(ds, ["PROFILE_NUMBER", "profile_number"])
    pres_v = safe_get_var(ds, ["PRES", "pressure"])
    temp_v = safe_get_var(ds, ["TEMP", "temperature"])
    sal_v = safe_get_var(ds, ["PSAL", "salinity"])
    doxy_v = safe_get_var(ds, ["DOXY", "doxy", "DOXY02"])

    # normalize function -> returns list of per-profile items
    def norm(x):
        if x is None:
            return []
        a = np.array(x)
        if a.ndim == 0:
            return [a.item()]
        if a.ndim == 1:
            return a.tolist()
        # ndim >=2 : treat as [nprof, nlevels]
        return [a[i,:] for i in range(a.shape[0])]

    lat_n = norm(lat_v)
    lon_n = norm(lon_v)
    juld_n = norm(juld_v)
    cycle_n = norm(cycle_v)
    profnum_n = norm(profnum_v)
    pres_n = norm(pres_v)
    temp_n = norm(temp_v)
    sal_n = norm(sal_v)
    doxy_n = norm(doxy_v)

    # determine number of profiles
    counts = [len(x) for x in [lat_n, lon_n, juld_n, cycle_n, profnum_n, pres_n, temp_n, sal_n] if x]
    nprof = max(counts) if counts else 0
    if nprof == 0:
        # attempt single-profile fallback (1)
        nprof = 1

    profiles = []
    for i in range(nprof):
        # helper get element or None
        def get_arr(lst, idx):
            try:
                if lst == []:
                    return None
                return lst[idx]
            except Exception:
                return None

        cycle = get_arr(cycle_n, i)
        profnum = get_arr(profnum_n, i)
        lat = get_arr(lat_n, i)
        lon = get_arr(lon_n, i)
        juld = get_arr(juld_n, i)
        pres = get_arr(pres_n, i)
        temp = get_arr(temp_n, i)
        sal = get_arr(sal_n, i)
        doxy = get_arr(doxy_n, i)

        # safe convert scalars to Python numbers or leave as-is
        try:
            if isinstance(cycle, np.ndarray) and cycle.size==1:
                cycle = int(np.array(cycle).item())
            elif isinstance(cycle, (np.generic, float, int)):
                cycle = int(cycle)
        except:
            cycle = None

        try:
            if isinstance(profnum, np.ndarray) and profnum.size==1:
                profnum = int(np.array(profnum).item())
            elif isinstance(profnum, (np.generic, float, int)):
                profnum = int(profnum)
        except:
            profnum = None

        # juld -> datetime or None
        juld_dt = None
        if juld is not None:
            try:
                val = np.array(juld).flatten()[0] if hasattr(juld, "shape") else juld
                valf = float(val)
                juld_dt = datetime(1950,1,1) + timedelta(days=valf)
            except Exception:
                juld_dt = None

        # lat lon to floats or None
        latf = to_number_or_none(lat if not hasattr(lat, "shape") else (np.array(lat).flatten()[0] if np.array(lat).size>0 else None))
        lonf = to_number_or_none(lon if not hasattr(lon, "shape") else (np.array(lon).flatten()[0] if np.array(lon).size>0 else None))

        # Convert level arrays into python lists and align lengths
        def to_list_safe(x):
            if x is None:
                return []
            a = np.array(x)
            if a.ndim == 0:
                return [a.item()]
            return a.tolist()

        pres_list = to_list_safe(pres)
        temp_list = to_list_safe(temp)
        sal_list = to_list_safe(sal)
        doxy_list = to_list_safe(doxy) if doxy is not None else [None]*len(pres_list)

        # pad to same length
        L = max(len(pres_list), len(temp_list), len(sal_list), len(doxy_list))
        def pad(lst):
            if lst is None:
                return [None]*L
            if len(lst) < L:
                return lst + [None]*(L - len(lst))
            return lst[:L]

        pres_list = pad(pres_list)
        temp_list = pad(temp_list)
        sal_list = pad(sal_list)
        doxy_list = pad(doxy_list)

        # convert numbers where possible else None
        pres_final = [to_number_or_none(x) for x in pres_list]
        temp_final = [to_number_or_none(x) for x in temp_list]
        sal_final = [to_number_or_none(x) for x in sal_list]
        doxy_final = [to_number_or_none(x) for x in doxy_list]

        profile_obj = {
            "cycle": int(cycle) if cycle is not None else 0,
            "profile_number": int(profnum) if profnum is not None else 0,
            "juld": juld_dt,
            "lat": latf,
            "lon": lonf,
            "depth": pres_final,
            "temp": temp_final,
            "sal": sal_final,
            "doxy": doxy_final
        }
        profiles.append(profile_obj)

    ds.close()
    return profiles

# ------------ DB helpers ------------
def insert_float_meta(conn, float_id, meta_json, home_dac):
    try:
        meta_text = json.dumps(meta_json)
    except Exception:
        meta_text = json.dumps(deep_clean(meta_json))
    sql = text("""
        INSERT INTO floats(float_id, home_dac, meta, last_update)
        VALUES(:fid, :dac, :meta::jsonb, now())
        ON CONFLICT (float_id) DO UPDATE
        SET meta = EXCLUDED.meta,
            home_dac = EXCLUDED.home_dac,
            last_update = now();
    """)
    conn.execute(sql, {"fid": float_id, "dac": home_dac, "meta": meta_text})

def upsert_profile(conn, float_id, cycle, profile_number, juld, lat, lon, profile_meta=None):
    uid = f"{float_id}:{int(cycle)}"
    meta_text = json.dumps(profile_meta or {})
    sql = text("""
        INSERT INTO profiles(uid, float_id, cycle, profile_number, juld, lat, lon, profile_meta, geom, updated_at)
        VALUES(:uid, :fid, :cycle, :pnum, :juld, :lat, :lon, :meta::jsonb, ST_SetSRID(ST_MakePoint(:lon, :lat), 4326), now())
        ON CONFLICT (uid) DO UPDATE
        SET profile_number = EXCLUDED.profile_number,
            juld = EXCLUDED.juld,
            lat = EXCLUDED.lat,
            lon = EXCLUDED.lon,
            profile_meta = EXCLUDED.profile_meta,
            geom = EXCLUDED.geom,
            updated_at = now()
        RETURNING id;
    """)
    row = conn.execute(sql, {"uid": uid, "fid": float_id, "cycle": int(cycle), "pnum": int(profile_number or 0),
                              "juld": juld, "lat": lat, "lon": lon, "meta": meta_text}).fetchone()
    return row[0]

def bulk_insert_measurements(conn, profile_id, profile_number, depths, temps, sals, doxies):
    # Prepare rows, skipping rows without valid depth
    rows = []
    for d,t,s,dx in zip(depths, temps, sals, doxies):
        if d is None:
            continue
        row = {
            "profile_id": profile_id,
            "profile_number": profile_number,
            "depth": float(d),
            "temp": None if t is None else float(t),
            "sal": None if s is None else float(s),
            "doxy": None if dx is None else float(dx)
        }
        rows.append(row)
    if not rows:
        return
    insert_sql = text("""
        INSERT INTO measurements(profile_id, profile_number, depth, temp, sal, doxy, qc_flags, created_at)
        VALUES(:profile_id, :profile_number, :depth, :temp, :sal, :doxy, '{}'::jsonb, now())
        ON CONFLICT (profile_id, depth) DO NOTHING;
    """)
    # chunked executemany
    for i in range(0, len(rows), MEASUREMENT_CHUNK):
        chunk = rows[i:i+MEASUREMENT_CHUNK]
        conn.execute(insert_sql, chunk)

# ------------ Listing profiles on DAC ------------
def list_profiles_on_dac(dac, float_id):
    url = f"{DAC_ROOT}/{dac}/{float_id}/profiles/"
    try:
        r = requests.get(url, timeout=15)
        if r.status_code != 200:
            return []
        files = []
        for line in r.text.splitlines():
            if ".nc" in line and "_meta" not in line:
                # crude href extraction
                if "href" in line.lower():
                    import re
                    m = re.findall(r'href=[\'"]?([^\'" >]+)', line, flags=re.IGNORECASE)
                    if m:
                        for n in m:
                            if n.endswith(".nc") and "_meta" not in n:
                                files.append(n)
                else:
                    parts = line.split()
                    for p in parts:
                        if p.endswith(".nc") and "_meta" not in p:
                            files.append(p.strip('">,'))
        return sorted(list(dict.fromkeys(files)))
    except Exception as e:
        logger.debug(f"list_profiles error: {e}")
        return []

# ------------ Main ingest for one float ------------
def ingest_float_from_dac(dac, float_id, base_dir=BASE_DIR):
    logger.info(f"Starting ingest {float_id} (DAC={dac})")
    float_dir = os.path.join(base_dir, dac, float_id)
    ensure_dir(float_dir)

    meta_url = f"{DAC_ROOT}/{dac}/{float_id}/{float_id}_meta.nc"
    meta_local = os.path.join(float_dir, f"{float_id}_meta.nc")
    download_if_needed(meta_url, meta_local)

    meta_json = {}
    if os.path.exists(meta_local):
        try:
            meta_json = read_meta_file(meta_local)
        except Exception as e:
            logger.warning(f"Meta parse failed: {e}")

    # insert meta
    try:
        with engine.begin() as conn:
            insert_float_meta(conn, float_id, meta_json, dac)
    except SQLAlchemyError as e:
        logger.error(f"DB error insert float meta: {e}")

    # profiles listing + download
    profiles = list_profiles_on_dac(dac, float_id)
    logger.info(f"Discovered {len(profiles)} profiles for {float_id}")

    profiles_dir = os.path.join(float_dir, "profiles")
    ensure_dir(profiles_dir)
    local_paths = []
    for fname in tqdm(profiles, desc=f"Downloading {float_id}"):
        url = f"{DAC_ROOT}/{dac}/{float_id}/profiles/{fname}"
        local = os.path.join(profiles_dir, fname)
        ok = download_if_needed(url, local)
        if ok and os.path.exists(local):
            local_paths.append(local)

    # parse & insert
    for local in tqdm(local_paths, desc=f"Ingesting {float_id}"):
        try:
            parsed = parse_profile_file(local)
        except Exception as e:
            logger.error(f"Parse failure {local}: {e}")
            continue
        with engine.begin() as conn:
            for p in parsed:
                try:
                    pid = upsert_profile(conn, float_id, p["cycle"], p["profile_number"], p["juld"], p["lat"], p["lon"], profile_meta={})
                    bulk_insert_measurements(conn, pid, p["profile_number"], p["depth"], p["temp"], p["sal"], p["doxy"])
                except Exception as e:
                    logger.exception(f"DB insert profile/measurements error for {local}: {e}")
                    continue
    logger.info(f"Completed ingest for {float_id}")

# ------------ CLI ------------
def main():
    if len(sys.argv) < 2:
        print("Usage: python ocean_ingest_v3.py <float_id> OR ALL")
        return
    arg = sys.argv[1].strip()
    if arg.upper() == "ALL":
        if not os.path.exists("floats.txt"):
            print("floats.txt missing (one float id per line)")
            return
        floats = [l.strip() for l in open("floats.txt").read().splitlines() if l.strip()]
    else:
        floats = [arg]
    for fid in floats:
        try:
            ingest_float_from_dac(DEFAULT_DAC, fid, BASE_DIR)
        except KeyboardInterrupt:
            logger.warning("Interrupted by user")
            break
        except Exception as e:
            logger.exception(f"Unhandled error for {fid}: {e}")

if __name__ == "__main__":
    main()
