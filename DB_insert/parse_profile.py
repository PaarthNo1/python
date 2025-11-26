# parse_profile.py

import os
import requests
import xarray as xr
import numpy as np
import pandas as pd

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)


def download_to_file(url, out_dir=DATA_DIR, timeout=40):
    fname = os.path.basename(url)
    path = os.path.join(out_dir, fname)

    # Use cached file if valid
    if os.path.exists(path) and os.path.getsize(path) > 0:
        print(f"⚡ Using cached file: {path}")
        return path

    print(f"📥 Downloading: {url}")
    try:
        with requests.get(url, stream=True, timeout=timeout) as r:
            r.raise_for_status()
            with open(path, "wb") as fh:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        fh.write(chunk)
        print(f"✅ Saved to: {path}")
        return path
    except Exception as e:
        print(f"❌ Download failed ({url}): {e}")
        return None


# ----------------- FAST HELPERS -----------------

def fast_extract_string(arr):
    """
    Extract string from NetCDF character arrays or byte arrays.
    Much faster and cleaner than the old extract_float_id().
    """
    try:
        # Flatten & join if bytes
        if isinstance(arr, np.ndarray) and arr.dtype.type in (np.bytes_, np.uint8):
            return b"".join(arr.flatten()).decode("utf-8", "ignore").strip()
    except:
        pass

    # Fallback: convert each item to str
    try:
        flat = arr.flatten()
        return "".join(str(x) for x in flat).strip()
    except:
        return str(arr).strip()


def fast_int_first(val):
    """Fast extraction of first element or scalar → int."""
    try:
        return int(val[0]) if np.ndim(val) > 0 else int(val)
    except:
        return None


def fast_first(val):
    """Return scalar or first item safely."""
    try:
        return val[0] if np.ndim(val) > 0 else val
    except:
        return val


def sanitize_lat_lon(lat_val, lon_val):
    """Quick and safe conversion to valid floats."""
    try:
        lat = float(lat_val)
        lon = float(lon_val)
    except:
        return None, None

    if np.isnan(lat) or np.isnan(lon):
        return None, None

    if abs(lat) > 90 or abs(lon) > 180:
        print(f"⚠ Invalid lat/lon detected ({lat}, {lon}) → NULL")
        return None, None

    return lat, lon


# ----------------- MAIN PARSER -----------------

def parse_profile(profile_url):
    """
    Fast profile parser. Returns:
    float_id, cycle, profile_number, latitude, longitude, juld, source_file, profile_path
    """
    ppath = download_to_file(profile_url)
    if not ppath:
        raise RuntimeError("Profile download failed")

    # Fast open without CF decoding overhead
    ds = xr.open_dataset(
        ppath,
        decode_cf=False,
        mask_and_scale=False,
        decode_times=False
    )

    # FLOAT ID
    float_id = None
    if "PLATFORM_NUMBER" in ds.variables:
        try:
            float_id = fast_extract_string(ds["PLATFORM_NUMBER"].values)
        except:
            float_id = None

    # CYCLE NUMBER
    cycle = None
    if "CYCLE_NUMBER" in ds.variables:
        cycle = fast_int_first(ds["CYCLE_NUMBER"].values)

    # PROFILE NUMBER (fallback to cycle)
    profile_number = None
    if "PROFILE_NUMBER" in ds.variables:
        profile_number = fast_int_first(ds["PROFILE_NUMBER"].values)
    else:
        profile_number = cycle

    # LAT/LON
    lat = lon = None
    if "LATITUDE" in ds.variables and "LONGITUDE" in ds.variables:
        lat_raw = fast_first(ds["LATITUDE"].values)
        lon_raw = fast_first(ds["LONGITUDE"].values)
        lat, lon = sanitize_lat_lon(lat_raw, lon_raw)

    # JULD → Timestamp
    juld_ts = None
    if "JULD" in ds.variables:
        try:
            jval = fast_first(ds["JULD"].values)
            if not np.isnan(jval):
                origin = pd.Timestamp("1950-01-01", tz="UTC")
                juld_ts = origin + pd.to_timedelta(float(jval), unit="D")
        except:
            juld_ts = None

    source_file = os.path.basename(profile_url)

    return {
        "float_id": float_id,
        "cycle": cycle,
        "profile_number": profile_number,
        "latitude": lat,
        "longitude": lon,
        "juld": juld_ts,
        "source_file": source_file,
        "profile_path": ppath
    }

