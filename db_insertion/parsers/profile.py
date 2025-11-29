# parse_profile.py
import os
import xarray as xr
import numpy as np
import pandas as pd
from dataset_cache import CACHE
import requests  # kept for backward compatibility if other code uses download_to_file

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)


def download_to_file(url, out_dir=DATA_DIR, timeout=40):
    """
    Backwards-compatible downloader (kept for other modules that might call it).
    We prefer using CACHE.ensure_file / CACHE.get_dataset in new code.
    """
    fname = os.path.basename(url)
    path = os.path.join(out_dir, fname)

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
    Works with:
      - numpy bytes arrays (2D char arrays)
      - arrays of bytes (np.ndarray with dtype np.bytes_)
      - numeric ASCII arrays (rare)
      - fallback to join of flattened values
    """
    try:
        if isinstance(arr, np.ndarray) and arr.dtype.kind in ("S", "U", "b", "B"):
            # bytes-like or fixed-length string array
            # flatten then join bytes/strings
            flat = arr.flatten()
            # if bytes, join bytes
            if flat.dtype.type is np.bytes_ or flat.dtype.kind == "S":
                return b"".join(flat).decode("utf-8", "ignore").strip()
            # otherwise join string representations
            return "".join(str(x) for x in flat).strip()
    except Exception:
        pass

    # Fallbacks
    try:
        flat = np.asarray(arr).flatten()
        return "".join(str(x) for x in flat).strip()
    except Exception:
        return str(arr).strip()


def fast_int_first(val):
    """Fast extraction of first element or scalar → int (or None)."""
    try:
        return int(val[0]) if np.ndim(val) > 0 else int(val)
    except Exception:
        return None


def fast_first(val):
    """Return scalar or first item safely."""
    try:
        return val[0] if np.ndim(val) > 0 else val
    except Exception:
        return val


def sanitize_lat_lon(lat_val, lon_val):
    """Quick and safe conversion to valid floats; invalid -> (None, None)."""
    try:
        lat = float(lat_val)
        lon = float(lon_val)
    except Exception:
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
    Fast profile parser.
    Returns dict with keys:
      float_id, cycle, profile_number, latitude, longitude, juld, source_file, profile_path
    Raises exceptions when dataset cannot be obtained (caller should handle).
    """
    # 1) ensure file exists on disk and capture path (so we can return profile_path)
    #    CACHE.ensure_file will download if needed and raise on permanent failures
    ppath = CACHE.ensure_file(profile_url)

    # 2) open or get dataset from cache
    #    get_dataset will open dataset (and cache it) and may raise on open failure
    ds = CACHE.get_dataset(profile_url, decode_cf=False, mask_and_scale=False, decode_times=False)

    # FLOAT ID
    float_id = None
    if "PLATFORM_NUMBER" in ds.variables:
        try:
            float_id = fast_extract_string(ds["PLATFORM_NUMBER"].values)
        except Exception:
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
    latitude = longitude = None
    if "LATITUDE" in ds.variables and "LONGITUDE" in ds.variables:
        lat_raw = fast_first(ds["LATITUDE"].values)
        lon_raw = fast_first(ds["LONGITUDE"].values)
        latitude, longitude = sanitize_lat_lon(lat_raw, lon_raw)

    # JULD → Timestamp (ARGO epoch 1950-01-01)
    juld = None
    if "JULD" in ds.variables:
        try:
            jval = fast_first(ds["JULD"].values)
            if not np.isnan(jval):
                origin = pd.Timestamp("1950-01-01", tz="UTC")
                # Fix: Use to_datetime with naive origin
                juld = pd.to_datetime(float(jval), unit="D", origin=pd.Timestamp("1950-01-01"))
        except Exception:
            juld = None

    source_file = os.path.basename(profile_url)

    # return same keys your other code expects (do not rename)
    return {
        "float_id": float_id,
        "cycle": cycle,
        "profile_number": profile_number,
        "latitude": latitude,
        "longitude": longitude,
        "juld": juld,
        "source_file": source_file,
        "profile_path": ppath
    }
