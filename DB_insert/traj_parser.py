# traj.parser.py

import xarray as xr
import numpy as np
import pandas as pd
from utils import download_nc, clean_bytes


# ------------------ FAST FLOAT ID DECODER ------------------
def fast_extract_float_id(raw):
    arr = np.array(raw).flatten()

    if arr.size == 0:
        return None

    first = arr[0]

    # bytes/int arrays
    if isinstance(first, (bytes, np.bytes_)):
        try:
            return (
                b"".join(arr)
                .decode("utf-8", "ignore")
                .replace("\x00", "")
                .strip()
            )
        except:
            pass

    if np.issubdtype(arr.dtype, np.integer):
        try:
            return "".join(chr(int(x)) for x in arr if 32 <= x <= 126).strip()
        except:
            pass

    # fallback
    return "".join(str(x) for x in arr).replace("\x00", "").strip()



# ------------------ SUPER FAST TRAJECTORY PARSER ------------------
def parse_traj_nc(url):
    """
    Ultra-fast vectorized parser for *_Rtraj.nc or *_Dtraj.nc
    Returns list of dict rows.
    """

    nc_path = download_nc(url)
    ds = xr.open_dataset(nc_path, decode_cf=False, mask_and_scale=False, decode_times=False)

    # Core arrays
    lat = ds["LATITUDE"].values.astype(float)
    lon = ds["LONGITUDE"].values.astype(float)
    juld = ds["JULD"].values.astype(float)
    cycles = ds["CYCLE_NUMBER"].values.astype(int)

    # Optional arrays
    pos_qc = ds["POSITION_QC"].values if "POSITION_QC" in ds else None
    pos_sys = ds["POSITIONING_SYSTEM"].values if "POSITIONING_SYSTEM" in ds else None

    # float_id
    float_id = fast_extract_float_id(ds["PLATFORM_NUMBER"].values)

    # ----------- 1) FILTER invalid rows vectorized -----------
    valid_mask = (np.abs(lat) <= 90) & (np.abs(lon) <= 180)
    lat = lat[valid_mask]
    lon = lon[valid_mask]
    juld = juld[valid_mask]
    cycles = cycles[valid_mask]

    if pos_qc is not None:
        pos_qc = pos_qc[valid_mask]
    if pos_sys is not None:
        pos_sys = pos_sys[valid_mask]

    n = len(lat)
    if n == 0:
        print("✔ Parsed 0 trajectory rows (no valid positions)")
        return []

    # ----------- 2) Vectorized timestamp conversion -----------
    origin = pd.Timestamp("1950-01-01", tz="UTC")
    juld_ts = origin + pd.to_timedelta(juld, unit="D")

    # ----------- 3) Vectorized profile number logic -----------
    profile_number = np.where(cycles >= 0, cycles, -1)

    # ----------- 4) Vectorized QC -----------
    if pos_qc is not None:
        qc_clean = np.array([clean_bytes(x) or "UNKNOWN" for x in pos_qc])
    else:
        qc_clean = np.array(["UNKNOWN"] * n)

    # ----------- 5) Vectorized POSITIONING_SYSTEM -----------
    if pos_sys is not None:
        # drift cycles = -1
        drift_mask = cycles == -1

        sys_clean = np.array([clean_bytes(x) or "UNKNOWN" for x in pos_sys])
        sys_clean[drift_mask] = "DRIFT"
    else:
        # all unknown system
        sys_clean = np.array(["UNKNOWN"] * n)

    src = url.split("/")[-1]

    # ----------- 6) Build vectorized dict rows (fast) -----------
    rows = []
    for i in range(n):
        rows.append({
            "float_id": float_id,
            "cycle": int(cycles[i]),
            "profile_number": int(profile_number[i]),
            "juld": juld_ts[i],
            "lat": float(lat[i]),
            "lon": float(lon[i]),
            "position_qc": qc_clean[i],
            "location_system": sys_clean[i],
            "source_file": src,
        })

    print(f"✔ Parsed {len(rows)} trajectory rows (FAST vectorized mode)")
    return rows
