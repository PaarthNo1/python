# traj_parser.py  (FINAL CRASH-PROOF VERSION)

import numpy as np
import pandas as pd
from dataset_cache import CACHE

def clean_bytes(x):
    if isinstance(x, (bytes, np.bytes_)):
        return x.decode("utf-8", errors="ignore").strip()
    if isinstance(x, (list, np.ndarray)):
        out = []
        for v in x:
            if isinstance(v, (bytes, np.bytes_)):
                out.append(v.decode("utf-8", errors="ignore").strip())
            else:
                out.append(str(v).strip())
        return "".join(out).strip()
    return str(x).strip()

def fast_extract_float_id(raw):
    arr = np.array(raw).flatten()
    if arr.size == 0:
        return None

    first = arr[0]
    if isinstance(first, (bytes, np.bytes_)):
        try:
            return b"".join(arr).decode("utf-8", "ignore").replace("\x00", "").strip()
        except:
            pass

    if np.issubdtype(arr.dtype, np.integer):
        try:
            return "".join(chr(int(x)) for x in arr if x > 32).strip()
        except:
            pass

    # ✔ FIXED LINE
    return "".join(str(x) for x in arr).replace("\x00", "").strip()


def parse_traj_nc(url):
    ds = CACHE.get_dataset(url, decode_cf=False, mask_and_scale=False, decode_times=False)

    lat = ds["LATITUDE"].values.astype(float)
    lon = ds["LONGITUDE"].values.astype(float)
    juld = ds["JULD"].values.astype(float)
    cycles = ds["CYCLE_NUMBER"].values.astype(int)

    float_id = fast_extract_float_id(ds["PLATFORM_NUMBER"].values)

    valid_mask = (np.abs(lat) <= 90) & (np.abs(lon) <= 180)
    lat = lat[valid_mask]
    lon = lon[valid_mask]
    juld = juld[valid_mask]
    cycles = cycles[valid_mask]

    N = len(lat)
    if N == 0:
        print("✔ No valid trajectory rows")
        return []

    origin = pd.Timestamp("1950-01-01")
    # Use to_datetime directly with origin to avoid Timedelta overflow
    juld_ts = pd.to_datetime(juld, unit="D", origin=origin)

    profile_num = np.where(cycles >= 0, cycles, -1)

    def safe_extract_array(var_name, fill="UNKNOWN"):
        if var_name not in ds:
            return np.array([fill] * N)

        raw = ds[var_name].values
        raw = np.asarray(raw)

        if raw.size != len(valid_mask):
            return np.array([fill] * N)

        try:
            raw = raw[valid_mask]
            return np.array([clean_bytes(x) if clean_bytes(x) else fill for x in raw])
        except:
            return np.array([fill] * N)

    pos_qc = safe_extract_array("POSITION_QC")
    
    # Handle MEASUREMENT_CODE (convert to int, fill with -1 or NULL if missing)
    mc_raw = safe_extract_array("MEASUREMENT_CODE", fill="999") # 999 as temporary fill
    measurement_code = []
    for x in mc_raw:
        try:
            measurement_code.append(int(float(x)))
        except:
            measurement_code.append(None)
    measurement_code = np.array(measurement_code)

    # Handle POSITIONING_SYSTEM (often global or single string)
    location_system_val = "UNKNOWN"
    if "POSITIONING_SYSTEM" in ds:
        try:
            raw_ps = ds["POSITIONING_SYSTEM"].values
            # If it's a char array (e.g. shape (8,)), join it
            if raw_ps.ndim == 1 and raw_ps.size < 100: # Heuristic: small size = single string
                 location_system_val = clean_bytes(raw_ps)
            elif raw_ps.ndim == 0:
                 location_system_val = clean_bytes(raw_ps)
            # If it matches N, use safe_extract_array (handled below if we wanted per-row, but usually it's global)
        except:
            pass
            
    # If we found a global value, use it for all rows
    pos_sys = np.array([location_system_val] * N)

    # New Recommended Columns (Kept only valid ones)
    sat_name  = safe_extract_array("SATELLITE_NAME", fill="UNKNOWN")
    juld_qc   = safe_extract_array("JULD_QC", fill="9") # 9 = missing

    src = url.split("/")[-1]

    rows = []
    for i in range(N):
        rows.append({
            "float_id": float_id,
            "cycle": int(cycles[i]),
            "profile_number": int(profile_num[i]),
            "juld": juld_ts[i],
            "lat": float(lat[i]),
            "lon": float(lon[i]),
            "position_qc": pos_qc[i],
            "location_system": pos_sys[i],
            "measurement_code": int(measurement_code[i]) if measurement_code[i] is not None else None,
            "satellite_name": sat_name[i],
            "juld_qc": juld_qc[i],
            "source_file": src,
        })

    print(f"✔ Parsed {len(rows)} trajectory rows (SAFE MODE)")
    return rows
