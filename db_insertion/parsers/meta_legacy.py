
import os
import numpy as np
import xarray as xr
import json
import requests
from dataset_cache import CACHE

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)


def download_to_file(url):
    fname = url.split("/")[-1]
    path = os.path.join(DATA_DIR, fname)

    # Use cache if file exists
    if os.path.exists(path):
        print(f"⚡ Using cached file: {path}")
        return path

    print(f"📥 Downloading: {url}")
    r = requests.get(url, timeout=30)
    r.raise_for_status()

    with open(path, "wb") as f:
        f.write(r.content)

    return path


# ---------- FAST HELPERS ----------

def clean_text(v):
    """Remove null-bytes and convert to clean string."""
    if v is None:
        return None
    return str(v).replace("\x00", "").strip()


def decode_bytes_fast(arr):
    """Decode byte arrays quickly and safely."""
    try:
        # fast path: directly decode
        return clean_text(b"".join(arr).decode("utf-8", "ignore"))
    except Exception:
        # fallback: convert each element to char
        try:
            return clean_text("".join(chr(int(x)) for x in arr))
        except:
            return clean_text(str(arr))


def safe_value_to_text(value):
    """
    Convert xarray/numpy values to a safe text representation:
    - bytes → decode
    - small arrays → JSON
    - scalars → string
    """
    # numpy bytes array
    if isinstance(value, np.ndarray) and value.dtype.type is np.bytes_:
        return decode_bytes_fast(value.flatten())

    # scalar
    if not isinstance(value, np.ndarray):
        return clean_text(value)

    # small array → JSON
    if value.size <= 50:
        try:
            return json.dumps(value.tolist())
        except:
            return clean_text(str(value))

    # large array → compact description instead of huge JSON
    return f"<array shape={value.shape} dtype={value.dtype}>"


# ---------- MAIN PARSER ----------

def parse_meta_nc(meta_url):
    # Fast dataset loading (cached)
    mds = CACHE.get_dataset(meta_url, decode_cf=False, mask_and_scale=False, decode_times=False)

    rows = []
    source_file = os.path.basename(meta_url)

    # 1) FLOAT ID  (FIXED ds → mds)
    float_id = None
    if "PLATFORM_NUMBER" in mds.variables:
        try:
            raw = mds["PLATFORM_NUMBER"].values
            float_id = decode_bytes_fast(raw)
        except:
            float_id = None

    # 2) GLOBAL ATTRIBUTES  (FIXED ds → mds)
    for k, v in mds.attrs.items():
        rows.append({
            "float_id": float_id,
            "var_name": "_GLOBAL",
            "attr_name": clean_text(k),
            "value_text": clean_text(v),
            "dtype": type(v).__name__,
            "shape": "scalar",
            "source_file": source_file
        })

    # 3) VARIABLES  (FIXED ds → mds)
    for var in mds.variables:
        v = mds[var]
        value_raw = v.values

        rows.append({
            "float_id": float_id,
            "var_name": var,
            "attr_name": None,
            "value_text": safe_value_to_text(value_raw),
            "dtype": str(v.dtype),
            "shape": str(v.shape),
            "source_file": source_file
        })

        # variable attributes
        for ak, av in v.attrs.items():
            rows.append({
                "float_id": float_id,
                "var_name": var,
                "attr_name": clean_text(ak),
                "value_text": clean_text(av),
                "dtype": type(av).__name__,
                "shape": "scalar",
                "source_file": source_file
            })

    return rows  # FIXED