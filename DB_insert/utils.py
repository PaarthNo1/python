# utils.py

import os
import requests
import numpy as np

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)


# ------------------------------
# CLEAN BYTE ARRAYS
# ------------------------------
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


# ------------------------------
# FLOAT ID extractor (shared everywhere)
# ------------------------------
def extract_float_id(raw):
    """
    Convert PLATFORM_NUMBER like [b'1', b'9', b'0', ...] → "1900042"
    """
    if raw is None:
        return None

    # case: array of bytes
    if isinstance(raw, (np.ndarray, list)):
        s = ""
        for v in raw:
            if isinstance(v, (bytes, np.bytes_)):
                v = v.decode("utf-8", errors="ignore")
            v = str(v).strip()
            if v and v not in ("\x00", " "):
                s += v
        return s.strip()

    # single value
    if isinstance(raw, (bytes, np.bytes_)):
        return raw.decode("utf-8", errors="ignore").strip()

    return str(raw).strip()


# ------------------------------
# NETCDF DOWNLOADER (central function)
# ------------------------------
def download_nc(url):
    """
    Download .nc file if not cached.
    Works for both: remote URL & local file paths.
    """
    # if local path:
    if os.path.exists(url):
        return url

    # remote URL case
    filename = os.path.join(DATA_DIR, url.split("/")[-1])
    if os.path.exists(filename):
        print(f"⚡ Using cached file: {filename}")
        return filename

    print(f"📥 Downloading: {url}")
    r = requests.get(url)
    if r.status_code != 200:
        raise Exception(f"Download failed: {url}")

    with open(filename, "wb") as f:
        f.write(r.content)

    return filename
