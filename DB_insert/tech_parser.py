# tech_parser.py

import xarray as xr
import numpy as np
import pandas as pd
from utils import download_nc


# ---------------- FAST HELPERS ----------------

def fast_decode_chars(arr):
    """
    Fast decoding of NetCDF char arrays.
    Handles:
    - bytes array
    - int ASCII array
    - mixed arrays
    """
    if arr is None:
        return None

    flat = np.array(arr).flatten()

    if flat.size == 0:
        return None

    first = flat[0]

    # Case 1: bytes-like
    if isinstance(first, (bytes, np.bytes_)):
        try:
            out = b"".join(flat).decode("utf-8", "ignore")
            return out.replace("\x00", "").strip()
        except:
            # fallback: decode per element
            try:
                out = "".join(
                    x.decode("utf-8", "ignore") if isinstance(x, (bytes, np.bytes_)) else str(x)
                    for x in flat
                )
                return out.replace("\x00", "").strip()
            except:
                return None

    # Case 2: integer ASCII (common in TECH file)
    if np.issubdtype(flat.dtype, np.integer):
        try:
            return "".join(chr(int(x)) for x in flat if x not in (0, 32)).strip()
        except:
            pass

    # Case 3: generic fallback
    try:
        out = "".join(str(x) for x in flat)
        return out.replace("\x00", "").strip()
    except:
        return None


def fast_extract_float_id(raw):
    """Fast PLATFORM_NUMBER extraction."""
    flat = np.array(raw).flatten()
    if flat.size == 0:
        return None

    first = flat[0]
    if isinstance(first, (bytes, np.bytes_)):
        try:
            return b"".join(flat).decode("utf-8", "ignore").replace("\x00", "").strip()
        except:
            pass

    if np.issubdtype(flat.dtype, np.integer):
        try:
            return "".join(chr(int(x)) for x in flat if x > 32).strip()
        except:
            pass

    try:
        return "".join(str(x) for x in flat).replace("\x00", "").strip()
    except:
        return None


def fast_parse_dt14(arr):
    """Parse DATE_CREATION (YYYYMMDDHHMMSS) quickly."""
    s = fast_decode_chars(arr)
    if not s or len(s) < 14:
        return None
    try:
        return pd.to_datetime(s[:14], format="%Y%m%d%H%M%S", utc=True)
    except:
        return None


def extract_units(param_name):
    """Same logic, but kept unchanged."""
    if not param_name:
        return None
    name = param_name.lower().strip()

    if name.endswith("_volt") or "battery_volt" in name:
        return "V"
    if name.endswith("_volts"):
        return "V"
    if name.endswith("_mv"):
        return "mV"
    if name.endswith("_ma"):
        return "mA"
    if name.endswith("_amp") or name.endswith("_amps"):
        return "A"
    if name.endswith("_dbar") or ("pressure" in name and "dbar" in name):
        return "dbar"
    if name.endswith("_mbar"):
        return "mbar"
    if name.endswith("_bar"):
        return "bar"
    if name.endswith("_inch") or name.endswith("_in"):
        return "inch"
    if name.endswith("_psi"):
        return "psi"
    if name.endswith("_mm"):
        return "mm"
    if name.endswith("_cm"):
        return "cm"
    if name.endswith("_sec") or name.endswith("_seconds"):
        return "second"
    if name.endswith("_ms"):
        return "ms"
    if name.endswith("_min"):
        return "minute"
    if "_count" in name:
        return "count"
    if "_sample" in name or "_samples" in name:
        return "samples"
    if "_cycle" in name or "_cycles" in name:
        return "cycles"
    if name.endswith("_byte"):
        return "byte"
    if name.endswith("_bit"):
        return "bit"
    if "temp" in name:
        return "degree_C"
    return None


# ---------------- MAIN PARSER ----------------

def parse_tech_nc(url):
    """
    Parse *_tech.nc into small dict rows.
    FAST optimized version.
    """

    nc_path = download_nc(url)
    ds = xr.open_dataset(nc_path, decode_cf=False, mask_and_scale=False, decode_times=False)

    # float id
    float_id = fast_extract_float_id(ds["PLATFORM_NUMBER"].values)

    # arrays
    names = ds["TECHNICAL_PARAMETER_NAME"].values
    values = ds["TECHNICAL_PARAMETER_VALUE"].values
    cycles = ds["CYCLE_NUMBER"].values

    # file-level timestamp
    collected_at = fast_parse_dt14(ds["DATE_CREATION"].values)

    N = len(cycles)
    rows = []

    # Pre-cast cycle array
    cycles_int = np.array(cycles).astype(int)

    # Fast decode all name/value arrays FIRST (vectorized loop)
    decoded_names = [fast_decode_chars(names[i]) for i in range(N)]
    decoded_values = [fast_decode_chars(values[i]) for i in range(N)]

    src = url.split("/")[-1]

    # Build final rows (minimal work inside loop)
    for i in range(N):
        pname = decoded_names[i]
        if not pname:
            continue

        pval = decoded_values[i]

        rows.append({
            "float_id": float_id,
            "cycle": cycles_int[i],
            "param_name": pname,
            "param_value": pval,
            "units": extract_units(pname),
            "collected_at": collected_at,
            "source_file": src
        })

    print(f"✔ Parsed {len(rows)} tech parameters with collected_at = DATE_CREATION")
    return rows
