import os
import requests
import warnings
import xarray as xr
import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy import text
import logging

# ---------------------------------------------------------
# SILENT MODE — No warnings, no SQLAlchemy spam
# ---------------------------------------------------------
warnings.filterwarnings("ignore")
logging.getLogger('sqlalchemy.engine').setLevel(logging.CRITICAL)
logging.getLogger('sqlalchemy.pool').setLevel(logging.CRITICAL)
logging.getLogger('sqlalchemy.dialects').setLevel(logging.CRITICAL)
logging.getLogger('sqlalchemy').setLevel(logging.CRITICAL)

# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------
DB_URL = "postgresql://postgres:simran%4004@localhost:5432/oceaniq_db"
NC_URL = "https://data-argo.ifremer.fr/dac/aoml/1900042/profiles/D1900042_001.nc"
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

# ---------------------------------------------------------
# DOWNLOAD NETCDF
# ---------------------------------------------------------
def download_nc(url):
    filename = url.split("/")[-1]
    filepath = os.path.join(DATA_DIR, filename)

    print(f"\n📥 Downloading: {filename}")

    response = requests.get(url)
    if response.status_code != 200:
        print("❌ Download failed:", response.status_code)
        return None

    with open(filepath, "wb") as f:
        f.write(response.content)

    print(f"✔ Download complete — saved at: {filepath}")
    return filepath

# ---------------------------------------------------------
# POSTGRES CONNECTION
# ---------------------------------------------------------
engine = sqlalchemy.create_engine(DB_URL, pool_pre_ping=True, future=True)

# ---------------------------------------------------------
# FLOAT ID DECODER
# ---------------------------------------------------------

def extract_float_id(raw_id):

    # Some files have shape (2,8) → take first row only
    raw_id = np.array(raw_id)
    if raw_id.ndim > 1:
        raw_id = raw_id[0]

    # Flatten row
    raw_id = raw_id.flatten()

    # Case A: bytes → decode
    if isinstance(raw_id[0], (bytes, np.bytes_)):
        return "".join([x.decode() for x in raw_id]).strip()

    # Case B: integers → convert ASCII
    if np.issubdtype(raw_id.dtype, np.integer):
        return "".join([chr(int(x)) for x in raw_id]).strip()

    # Fallback
    return "".join([str(x) for x in raw_id]).strip()


# ---------------------------------------------------------
# LOAD + PARSE + INSERT
# ---------------------------------------------------------
def load_to_postgres(nc_path):
    print(f"\n📄 Reading NetCDF file: {nc_path}")

    ds = xr.open_dataset(
        nc_path,
        decode_cf=False,
        mask_and_scale=False,
        decode_times=False
    )

    # FLOAT ID
    float_id = extract_float_id(ds["PLATFORM_NUMBER"].values)

    # BASIC VARIABLES
    lat = ds["LATITUDE"].values
    lon = ds["LONGITUDE"].values

    # REAL JULD TIME (ARGO format = days since 1950)
    juld = ds["JULD"].values
    origin = pd.Timestamp("1950-01-01")
    time = origin + pd.to_timedelta(juld, unit="D")

    # SENSOR VARIABLES
    temp = ds["TEMP"].values
    sal = ds["PSAL"].values
    pres = ds["PRES"].values

    # Replace 99999 with NaN
    temp = np.where(temp > 90000, np.nan, temp)
    sal = np.where(sal > 90000, np.nan, sal)
    pres = np.where(pres > 90000, np.nan, pres)

    # PROFILE METADATA
    cycle_number = ds["CYCLE_NUMBER"].values

    # Fallback if PROFILE_NUMBER missing
    if "PROFILE_NUMBER" in ds.variables:
        profile_number_var = ds["PROFILE_NUMBER"].values
    else:
        profile_number_var = cycle_number  # safe fallback

    num_profiles = temp.shape[0]

    print(f"🆔 Float ID: {float_id}")
    print(f"📌 Total Profiles: {num_profiles}")

    # =====================================================
    # INSERT BOTH TABLES
    # =====================================================
    with engine.begin() as conn:

        for prof in range(num_profiles):

            cycle = int(cycle_number[prof])
            profile_num = int(profile_number_var[prof])

            # INSERT FLOAT METADATA
            conn.execute(text("""
                INSERT INTO floats(float_id, cycle, profile_number, lat, lon, juld)
                VALUES(:f, :c, :p, :lat, :lon, :t)
                ON CONFLICT (float_id, cycle) DO NOTHING;
            """), {
                "f": float_id,
                "c": cycle,
                "p": profile_num,
                "lat": float(lat[prof]),
                "lon": float(lon[prof]),
                "t": time[prof]
            })

            # BUILD MEASUREMENTS
            rows = []
            for lvl in range(len(pres[prof])):
                rows.append({
                    "float_id": float_id,
                    "cycle": cycle,
                    "profile_number": profile_num,
                    "depth": float(pres[prof][lvl]),
                    "temp": float(temp[prof][lvl]) if not pd.isna(temp[prof][lvl]) else None,
                    "sal": float(sal[prof][lvl]) if not pd.isna(sal[prof][lvl]) else None
                })

            df = pd.DataFrame(rows)

            # SILENT bulk insert
            try:
                df.to_sql("measurements", engine, index=False, if_exists="append", method="multi")
            except Exception:
                pass

    print("✔ Data successfully uploaded to PostgreSQL!")

# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------
if __name__ == "__main__":
    print("\n=== 🌊 OceanIQ ARGO Loader (Cycle/Profile Fallback Version) ===")

    nc_file = download_nc(NC_URL)

    if nc_file:
        try:
            load_to_postgres(nc_file)
        except Exception as e:
            print("❌ Unexpected Error:", e)

    print("\n🎉 ALL DONE — Clean execution completed!")
