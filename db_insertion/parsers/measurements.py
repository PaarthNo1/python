
# parse_profile_measurements.py (optimized)
import os
import re
import requests
import xarray as xr
import numpy as np
import pandas as pd
from dataset_cache import CACHE


DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

# ---------------------------------------------------------
# DOWNLOAD (unchanged logic but explicit chunk size param)
# ---------------------------------------------------------
def download_to_file(url, out_dir=DATA_DIR, timeout=40):
    fname = os.path.basename(url)
    path = os.path.join(out_dir, fname)

    if os.path.exists(path) and os.path.getsize(path) > 0:
        return path

    print(f"📥 Downloading: {url}")
    try:
        with requests.get(url, stream=True, timeout=timeout) as r:
            r.raise_for_status()
            with open(path, "wb") as fh:
                for chunk in r.iter_content(8192):
                    if chunk:
                        fh.write(chunk)
        return path
    except Exception as e:
        print(f"❌ Download failed: {e}")
        return None

# ---------------------------------------------------------
# HELPERS (kept but optimized)
# ---------------------------------------------------------
def _is_fill_value(val):
    """ARGO missing/fill value detection (fast)."""
    try:
        v = float(val)
        if np.isnan(v) or abs(v) > 90000:
            return True
        return False
    except Exception:
        return True

def safe_float(x):
    """
    Convert a scalar-like value to float or None.
    This is a fast scalar helper used when element-level conversion is necessary.
    """
    if x is None:
        return None

    # masked arrays
    if isinstance(x, np.ma.MaskedArray):
        if x.mask:
            return None
        x = x.data

    # bytes
    if isinstance(x, (bytes, np.bytes_)):
        try:
            x = x.decode("utf-8", "ignore")
        except Exception:
            return None

    try:
        f = float(str(x).strip())
        if np.isnan(f) or abs(f) > 90000:
            return None
        return f
    except Exception:
        return None

# ---------------------------------------------------------
# VAR EXCLUSION (ERROR, STD, UNCERTAINTY)
# ---------------------------------------------------------
EXCLUDE_SUFFIXES = [
    "_ERROR", "_ADJUSTED_ERROR", "_STD", "_UNCERTAINTY",
    "_RAW", "_OFFSET", "_SLOPE"
]

def is_valid_sensor(varname):
    v = varname.upper()
    for suf in EXCLUDE_SUFFIXES:
        if v.endswith(suf):
            return False
    return True

# ---------------------------------------------------------
# SENSOR DETECTION
# ---------------------------------------------------------
SENSOR_PATTERNS = [
    r"^TEMP", r"^PSAL", r"^PRES", r"^DOXY",
    r"^CHLA", r"^BBP", r"^NITRATE",
    r"^PH", r"^O2", r"^IRRADIANCE",
    r"^BETA", r"^CP"
]

_sensor_regexes = [re.compile(p, re.IGNORECASE) for p in SENSOR_PATTERNS]

def looks_like_sensor(name):
    for rx in _sensor_regexes:
        if rx.search(name):
            return True
    return False

def normalize_sensor_name(name):
    s = name.lower()
    s = re.sub(r"_adjusted$", "", s)
    s = re.sub(r"\W+", "_", s)
    return s

# ---------------------------------------------------------
# MAIN PARSER (optimized)
# ---------------------------------------------------------
def parse_profile_measurements(profile_url, prefer_adjusted=True):
    """
    Reads one Argo profile file and returns a DataFrame with columns:
    float_id, cycle, profile_number, juld, latitude, longitude,
    depth_m, sensor, value, qc, source_file
    """

    # 2) Open dataset WITHOUT expensive CF decoding
    ds = CACHE.get_dataset(profile_url, decode_cf=False, mask_and_scale=False, decode_times=False)

    # ---------- Helper to grab first/scalar values quickly ----------
    def _first_scalar(v):
        # v is an xarray DataArray or ndarray-like
        try:
            a = np.array(v)
            return a[0] if a.ndim > 0 else a
        except Exception:
            return v

    # 3) ID and metadata (minimal calls)
    float_id = None
    if "PLATFORM_NUMBER" in ds:
        try:
            raw = np.array(ds["PLATFORM_NUMBER"].values).flatten()
            if raw.size > 0:
                if isinstance(raw[0], (bytes, np.bytes_)):
                    float_id = b"".join(raw).decode("utf-8", "ignore").strip()
                else:
                    float_id = "".join(str(x) for x in raw).strip()
        except Exception:
            float_id = None

    cycle = None
    if "CYCLE_NUMBER" in ds:
        try:
            val = _first_scalar(ds["CYCLE_NUMBER"].values)
            cycle = int(val)
        except Exception:
            cycle = None

    profile_number = None
    if "PROFILE_NUMBER" in ds:
        try:
            profile_number = int(_first_scalar(ds["PROFILE_NUMBER"].values))
        except Exception:
            profile_number = cycle
    else:
        profile_number = cycle

    lat = None; lon = None
    if "LATITUDE" in ds and "LONGITUDE" in ds:
        lat_raw = _first_scalar(ds["LATITUDE"].values)
        lon_raw = _first_scalar(ds["LONGITUDE"].values)
        # reuse safe_float for robust cleaning
        lat = safe_float(lat_raw)
        lon = safe_float(lon_raw)
        if lat is None or lon is None:
            lat = lon = None
        else:
            # validate ranges quickly
            if abs(lat) > 90 or abs(lon) > 180:
                print(f"⚠ Invalid lat/lon detected ({lat}, {lon}) → setting NULL")
                lat = lon = None

    # juld -> timestamp (first element)
    juld_ts = None
    if "JULD" in ds:
        try:
            jval = safe_float(_first_scalar(ds["JULD"].values))
            if jval is not None:
                origin = pd.Timestamp("1950-01-01", tz="UTC")
                # Fix: Use to_datetime with naive origin
                juld_ts = pd.to_datetime(float(jval), unit="D", origin=pd.Timestamp("1950-01-01"))
        except Exception:
            juld_ts = None

    # 4) PRES detection (first PRES-like variable)
    pres_var = None
    for name in ds.variables:
        if name.upper().startswith("PRES") and is_valid_sensor(name):
            pres_var = name
            break

    if pres_var is None:
        raise RuntimeError("No PRES variable found.")

    # Load pres array ONCE and normalize to 1D
    pres_arr_raw = np.array(ds[pres_var].values)
    pres = pres_arr_raw if pres_arr_raw.ndim == 1 else pres_arr_raw[0]
    n = int(len(pres))

    source_file = os.path.basename(profile_url)

    # 5) Build sensor_map efficiently: map base -> names (raw/adj/qc)
    sensor_map = {}
    varnames = list(ds.variables.keys())

    # Precompute available variable set for quick membership
    var_set = set(varnames)

    for var in varnames:
        # skip variables that are clearly not sensor-related
        if not looks_like_sensor(var):
            continue
        if not is_valid_sensor(var):
            continue

        vupper = var.upper()
        if vupper.endswith("_QC"):
            # QC handled by separate branch; skip direct mapping
            continue

        base = vupper.replace("_ADJUSTED", "")
        if base not in sensor_map:
            sensor_map[base] = {"raw": None, "adj": None, "qc": None}

        if vupper.endswith("_ADJUSTED"):
            sensor_map[base]["adj"] = var
        else:
            sensor_map[base]["raw"] = var

    # after building raw/adj mapping, attach qc names if present
    for base, info in sensor_map.items():
        qc_name = base + "_QC"
        if qc_name in var_set:
            sensor_map[base]["qc"] = qc_name
        else:
            sensor_map[base]["qc"] = None

    rows = []

    # 6) Prepare frequently used conversions to minimize python overhead
    # Pre-cast pres into float array with None for bad values
    pres_float = np.empty(n, dtype=float)
    pres_valid_mask = np.zeros(n, dtype=bool)
    for i in range(n):
        v = pres[i]
        f = safe_float(v)
        if f is None:
            pres_float[i] = np.nan
            pres_valid_mask[i] = False
        else:
            pres_float[i] = f
            pres_valid_mask[i] = True

    # 7) Iterate sensor_map and build rows using vectorized selection per sensor
    for base, info in sensor_map.items():
        raw_name = info["raw"]
        adj_name = info["adj"]
        qc_name = info["qc"]

        # Load arrays once and normalize to 1D numpy arrays
        def _get_arr(name):
            if name is None or name not in ds:
                return np.full(n, np.nan, dtype=float)
            a = np.array(ds[name].values)
            return a if a.ndim == 1 else a[0]

        raw_arr = _get_arr(raw_name)
        adj_arr = _get_arr(adj_name)
        qc_arr_raw = None
        if qc_name:
            # keep QC as object array for safe string extraction
            try:
                qa = np.array(ds[qc_name].values)
                qc_arr_raw = qa if qa.ndim == 1 else qa[0]
            except Exception:
                qc_arr_raw = np.array([None] * n, dtype=object)
        else:
            qc_arr_raw = np.array([None] * n, dtype=object)

        # Precompute qc string first char array (None if missing)
        qc_first = np.empty(n, dtype=object)
        for i in range(n):
            qv = qc_arr_raw[i]
            if qv is None or (isinstance(qv, float) and np.isnan(qv)):
                qc_first[i] = None
            else:
                # convert to str safely and take first char
                try:
                    s = str(qv).strip()
                    qc_first[i] = s[0] if len(s) > 0 else None
                except:
                    qc_first[i] = None

        # Prepare numeric arrays for raw/adj values using safe_float per element
        raw_vals = np.empty(n, dtype=float)
        adj_vals = np.empty(n, dtype=float)
        for i in range(n):
            raw_vals[i] = np.nan
            adj_vals[i] = np.nan
        for i in range(n):
            rv = raw_arr[i]
            av = adj_arr[i]
            raw_f = safe_float(rv)
            adj_f = safe_float(av)
            raw_vals[i] = raw_f if raw_f is not None else np.nan
            adj_vals[i] = adj_f if adj_f is not None else np.nan

        # Decide which indices to keep:
        # - depth must be valid
        # - value must be present (either raw or adj chosen)
        # Vectorized selection using masks
        for i in range(n):
            if not pres_valid_mask[i]:
                continue  # skip missing depth

            # choose value according to prefer_adjusted and QC
            qc_val = qc_first[i]
            raw_v = raw_vals[i]
            adj_v = adj_vals[i]

            use_adj = False
            if prefer_adjusted and (adj_name is not None):
                if qc_val in ("1", "2"):
                    # prefer adjusted when QC is good
                    if not np.isnan(adj_v):
                        use_adj = True

            if use_adj:
                val = None if np.isnan(adj_v) else float(adj_v)
            else:
                # prefer raw if available else adjusted
                if not np.isnan(raw_v):
                    val = float(raw_v)
                elif not np.isnan(adj_v):
                    val = float(adj_v)
                else:
                    val = None

            if val is None:
                continue

            depth = float(pres_float[i])
            qc_str = qc_val
            rows.append({
                "float_id": float_id,
                "cycle": cycle,
                "profile_number": profile_number,
                "juld": juld_ts,
                "latitude": lat,
                "longitude": lon,
                "depth_m": depth,
                "sensor": normalize_sensor_name(base),
                "value": val,
                "qc": qc_str,
                "source_file": source_file
            })

    df = pd.DataFrame(rows)
    # print(f"✔ Measurements parsed: {len(df)} rows (clean, no ERROR variables)")
    return df
