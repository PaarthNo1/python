# parse_meta.py  -- SUPER OPTIMIZED VERSION

import os
import requests
import xarray as xr
import numpy as np

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)


# ----------------------------- DOWNLOAD -----------------------------
def download_to_file(url, out_dir=DATA_DIR, timeout=40):
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
                for chunk in r.iter_content(8192):
                    if chunk:
                        fh.write(chunk)
        print(f"✅ Saved to: {path}")
        return path
    except Exception as e:
        print(f"❌ Download failed ({url}): {e}")
        return None


# ------------------------ FAST CHAR DECODER ------------------------
def _decode_char_array_fast(val):
    """
    Decode character arrays quickly:
    - bytes arrays → fast join+decode
    - int ASCII → fast vectorized map
    - fallback → simple str join
    """
    if val is None:
        return None

    arr = np.array(val).flatten()
    if arr.size == 0:
        return None

    first = arr[0]

    # bytes-like array
    if isinstance(first, (bytes, np.bytes_)):
        try:
            return b"".join(arr).decode("utf-8", "ignore").replace("\x00", "").strip()
        except Exception:
            pass

    # integer ASCII array
    if np.issubdtype(arr.dtype, np.integer):
        try:
            # vectorized int → char mapping
            return "".join(chr(int(x)) for x in arr).replace("\x00", "").strip()
        except Exception:
            pass

    # generic fallback
    try:
        return "".join(str(x) for x in arr).replace("\x00", "").strip()
    except Exception:
        return None


# ------------------- CANDIDATE META VARIABLES ---------------------
_WMO_NAMES = ["WMO_PLATFORM_CODE", "WMO", "WMO_ID", "PLATFORM_NUMBER", "PLATFORM_CODE"]
_PLATFORM_TYPE_NAMES = ["PLATFORM_TYPE", "PLATFORM_TYPE_NAME"]
_PROJECT_NAMES = ["PROJECT_NAME", "PROJECT", "SOURCE_PROJECT"]
_PI_NAMES = ["PI_NAME", "PI", "PRINCIPAL_INVESTIGATOR"]


def _find_first_existing_var(ds, candidates):
    """Return the first variable name that exists in dataset."""
    for name in candidates:
        if name in ds.variables:
            return name
    return None


# ----------------------------- MAIN PARSER -----------------------------
def parse_meta(meta_url):
    mp = download_to_file(meta_url)
    if not mp:
        raise RuntimeError("Meta download failed")

    mds = xr.open_dataset(mp, decode_cf=False, mask_and_scale=False, decode_times=False)

    # resolve variable names
    wmo_var = _find_first_existing_var(mds, _WMO_NAMES)
    platform_var = _find_first_existing_var(mds, _PLATFORM_TYPE_NAMES)
    project_var = _find_first_existing_var(mds, _PROJECT_NAMES)
    pi_var = _find_first_existing_var(mds, _PI_NAMES)

    # decode values safely & fast
    wmo_id = _decode_char_array_fast(mds[wmo_var].values) if wmo_var else None
    platform_type = _decode_char_array_fast(mds[platform_var].values) if platform_var else None
    project_name = _decode_char_array_fast(mds[project_var].values) if project_var else None
    pi_name = _decode_char_array_fast(mds[pi_var].values) if pi_var else None

    return {
        "wmo_id": wmo_id,
        "platform_type": platform_type,
        "project_name": project_name,
        "pi_name": pi_name,
        "meta_path": mp
    }
