# parse_profile_arrays.py  -- FINAL PRODUCTION VERSION (SAFE + FAST)

import os
import numpy as np
import xarray as xr
import pandas as pd
from dataset_cache import CACHE

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)


# =====================================================
# ⚡ COMMON HELPER FUNCTIONS  (NO CHANGE NEEDED)
# =====================================================

def remove_nulls(s):
    if s is None:
        return None
    return str(s).replace("\x00", "").strip()

def fast_decode_bytes(arr):
    try:
        a = np.asarray(arr).flatten()
        if a.size == 0:
            return None
        if isinstance(a[0], (bytes, np.bytes_)):
            return remove_nulls(b"".join(a).decode("utf-8", "ignore"))
        if np.issubdtype(a.dtype, np.integer):
            try:
                return remove_nulls("".join(chr(int(x)) for x in a))
            except:
                return remove_nulls("".join(str(int(x)) for x in a))
        return remove_nulls("".join(str(x) for x in a))
    except:
        return None

def fast_float_array(arr):
    a = np.asarray(arr)
    if np.ma.isMaskedArray(arr):
        a = arr.filled(np.nan)
    if a.ndim > 1:
        a = a.flatten()
    try:
        out = a.astype("float64", copy=True)
    except:
        result = []
        for v in a:
            try:
                fv = float(v)
                if np.isnan(fv) or abs(fv) > 90000:
                    result.append(None)
                else:
                    result.append(fv)
            except:
                result.append(None)
        return result

    mask = np.isnan(out) | (np.abs(out) > 90000)
    out[mask] = np.nan
    return [None if np.isnan(v) else float(v) for v in out]

def fast_qc_array(arr):
    a = np.asarray(arr)
    if a.ndim > 1:
        a = a.flatten()
    return [remove_nulls(x) if str(x).strip() else None for x in a]


# =====================================================
# ⚠ SAFE FLOAT EXTRACT (NO CRASH)
# =====================================================

def safe_first_float(ds, var):
    """⚠ SAFE — अगर missing या invalid हो → return None (no crash)"""
    if var not in ds:
        return None

    a = np.asarray(ds[var].values).flatten()
    if a.size == 0:
        return None

    try:
        f = float(a[0])
        if np.isnan(f) or abs(f) > 90000:
            return None
        return f
    except:
        return None


# =====================================================
# 🚀 MAIN PARSER (NOW FULLY SAFE)
# =====================================================

def parse_profile_arrays(profile_url):
    ds = CACHE.get_dataset(profile_url, decode_cf=False, mask_and_scale=False, decode_times=False)

    # FLOAT ID
    if "PLATFORM_NUMBER" not in ds:
        print(f"⚠ Missing PLATFORM_NUMBER → skipping arrays")
        return None
    
    # Handle 2D array (N_PROF, N_CHAR) -> take first profile
    plat_arr = ds["PLATFORM_NUMBER"].values
    if plat_arr.ndim > 1:
        plat_arr = plat_arr[0]
        
    float_id = fast_decode_bytes(plat_arr)

    # CYCLE NUMBER
    cycle = safe_first_float(ds, "CYCLE_NUMBER")
    if cycle is None:
        print(f"⚠ No CYCLE_NUMBER → skipping arrays")
        return None
    cycle = int(cycle)

    # PROFILE NUMBER (fallback)
    pno = safe_first_float(ds, "PROFILE_NUMBER")
    profile_number = int(pno) if pno is not None else cycle

    # LAT / LON / JULD — REQUIRED FOR DB
    lat = safe_first_float(ds, "LATITUDE")
    lon = safe_first_float(ds, "LONGITUDE")
    j   = safe_first_float(ds, "JULD")

    # ❗ اگر Required geo/time missing हो → skip पूरी file
    if lat is None or lon is None or j is None:
        print(f"⚠ Invalid geo/time → skipping arrays")
        return None

    # Fix: Use to_datetime with naive origin to handle large offsets
    juld = pd.to_datetime(j, unit="D", origin=pd.Timestamp("1950-01-01"))

    # 🔥 PRES always required but many floats have missing TEMP/PSAL
    if "PRES" not in ds.variables:
        print(f"⚠ No PRES found → skipping arrays")
        return None

    pres = fast_float_array(ds["PRES"].values)

    # OPTIONAL (SAFE FALLBACK)
    temp    = fast_float_array(ds["TEMP"].values)    if "TEMP" in ds else [None]*len(pres)
    psal    = fast_float_array(ds["PSAL"].values)    if "PSAL" in ds else [None]*len(pres)
    temp_qc = fast_qc_array(ds["TEMP_QC"].values)    if "TEMP_QC" in ds else [None]*len(pres)
    psal_qc = fast_qc_array(ds["PSAL_QC"].values)    if "PSAL_QC" in ds else [None]*len(pres)

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
        "source_file": remove_nulls(os.path.basename(profile_url)),
    }
