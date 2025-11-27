# parse_profile_arrays.py  -- SUPER OPTIMIZED VERSION

import os
import numpy as np
import xarray as xr
import requests
import pandas as pd

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

# ------------------- DOWNLOAD -------------------
def download_to_file(url):
    fname = url.split("/")[-1]
    path = os.path.join(DATA_DIR, fname)

    if os.path.exists(path) and os.path.getsize(path) > 0:
        print(f"⚡ Using cached file: {path}")
        return path

    print(f"📥 Downloading: {url}")
    r = requests.get(url, timeout=30)
    r.raise_for_status()

    with open(path, "wb") as f:
        f.write(r.content)

    return path


# ------------------- FAST HELPERS -------------------

def remove_nulls(s):
    """Remove null bytes and return clean string."""
    if s is None:
        return None
    return str(s).replace("\x00", "").strip()


def fast_decode_bytes(arr):
    """Fast decoding of PLATFORM_NUMBER arrays."""
    try:
        return remove_nulls(b"".join(arr.flatten()).decode("utf-8", "ignore"))
    except:
        try:
            return remove_nulls("".join(str(x) for x in arr.flatten()))
        except:
            return None


def fast_float_array(arr):
    """
    Fast conversion of numeric arrays:
    - Flatten to 1D
    - Convert to float
    - Replace invalid (fill values, NaN, >90000) with None
    """
    flat = arr.flatten() if arr.ndim > 1 else arr
    out = flat.astype("float64", copy=True)

    # Replace fill values and invalid entries with NaN
    mask = (np.isnan(out)) | (np.abs(out) > 90000)
    out[mask] = np.nan

    # Convert to python list with None instead of NaN
    return [None if np.isnan(v) else float(v) for v in out]


def fast_qc_array(arr):
    """
    Fast QC cleanup. QC arrays are typically char arrays.
    Convert to first char or None.
    """
    flat = arr.flatten() if arr.ndim > 1 else arr
    result = []

    for v in flat:
        try:
            s = remove_nulls(v)
            s = str(s).strip()
            result.append(s if s else None)
        except:
            result.append(None)

    return result


# ------------------- MAIN FUNCTION -------------------

def parse_profile_arrays(profile_url):
    path = download_to_file(profile_url)
    ds = xr.open_dataset(path, decode_cf=False, mask_and_scale=False, decode_times=False)

    # float id
    float_id = fast_decode_bytes(ds["PLATFORM_NUMBER"].values)

    # cycle
    cycle = int(ds["CYCLE_NUMBER"].values.flatten()[0])

    # profile number
    if "PROFILE_NUMBER" in ds:
        profile_number = int(ds["PROFILE_NUMBER"].values.flatten()[0])
    else:
        profile_number = cycle

    # lat/lon
    lat = float(ds["LATITUDE"].values.flatten()[0])
    lon = float(ds["LONGITUDE"].values.flatten()[0])

    # JULD → timestamp
    j = float(ds["JULD"].values.flatten()[0])
    juld = pd.Timestamp("1950-01-01", tz="UTC") + pd.to_timedelta(j, unit="D")

    # Extract arrays FAST
    pres = fast_float_array(ds["PRES"].values)
    temp = fast_float_array(ds["TEMP"].values)
    psal = fast_float_array(ds["PSAL"].values)

    temp_qc = fast_qc_array(ds["TEMP_QC"].values)
    psal_qc = fast_qc_array(ds["PSAL_QC"].values)

    source_file = remove_nulls(os.path.basename(profile_url))

    return {
        "float_id": remove_nulls(float_id),
        "cycle": cycle,
        "profile_number": profile_number,
        "juld": juld,
        "lat": lat,
        "lon": lon,
        "pres": pres,
        "temp": temp,
        "psal": psal,
        "temp_qc": temp_qc,
        "psal_qc": psal_qc,
        "source_file": source_file
    }
