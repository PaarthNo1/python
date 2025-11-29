import os
import sqlalchemy
from dotenv import load_dotenv
from auto_loader import auto_loader

# Load environment variables (so DB credentials are not hard-coded in code)
load_dotenv()

# Read DB URL from environment; fallback URL is used only if .env is missing
DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise ValueError("❌ DB_URL not found in .env file. Please set it.")

# Create a highly optimized SQLAlchemy engine with connection pooling.
# Pooling is critical when using remote databases like Aiven because
# opening new connections repeatedly adds 80–150ms network latency per query.
engine = sqlalchemy.create_engine(
    DB_URL,
    pool_pre_ping=True,   # Automatically checks and refreshes stale connections
    pool_size=5,          # Maintain 5 ready-to-use persistent connections
    max_overflow=10,      # Allow temporary extra connections during peak load
    pool_timeout=30,      # Timeout if the pool is busy for too long
    future=True           # Uses SQLAlchemy 2.0 style engine behavior
)

# Float ID being processed — kept exactly as given
# FLOAT_ID = [1902675, 2902203, 2903951, 4903875, 5907082, 5907179, 5907180, 6990618]

# FLOAT_ID = [7902200] #2025-11-10
# FLOAT_ID = [2902273, 2903953, 4903838, 4903876, 5907179, 7902250, 7902200] #2025-11-19
# FLOAT_ID = [1902676, 2903891, 4903776, 4903877, 6990617, 6990678, 6990716, 7901126, 7902242, 7902246, 7902247] #2025-11-20
# FLOAT_ID = [1902674, 2903954, 3902581, 3902668, 4903783, 4903873, 5907092, 5907138, 6990609, 6990611, 6990615, 6990705, 7901127, 7902287] #2025-11-21
# FLOAT_ID = [1902767, 2903892, 2903950, 3902669, 4903777, 4903869, 5907139, 5907171, 6990608, 6990614, 7901136, 7902243] #2025-11-22
# FLOAT_ID = [2902295, 2903894, 2903895, 2903988, 3902573, 3902629, 6990715, 7902244] #2025-11-23
# FLOAT_ID = [1902785, 2903893, 2903952, 4903837, 5907140, 5907176, 6990612, 7901130, 7902248] #2025-11-24
# FLOAT_ID = [1902670, 1902673, 2902296, 3902630, 4903874, 7902249, 7902251] #2025-11-25
# FLOAT_ID = [2902224, 2902306, 2903989, 5907085, 6990613, 6990616, 6990679, 7901131, 7902170] #2025-11-26 
# FLOAT_ID = [1902675, 2902203, 2903951, 4903875, 5907082, 5907180, 5907192, 6990618] #2025-11-27
# FLOAT_ID = [1902669, 1902671, 2902222, 2902272, 4903775, 5907083] #2025-11-28
# FLOAT_ID = [1902672, 2902223, 5907084, 6990610, 7901125, 7901128] #2025-11-29

FLOAT_ID = [1902043]

for selected in FLOAT_ID:
    selected = str(selected)  # force cast to string
    print(f"⚙ Running auto_loader for {selected} ...")
    auto_loader(selected, engine)



# Pass the pooled engine to the loader.
# This makes database operations significantly faster because
# connections are reused instead of recreated each time.
# auto_loader(FLOAT_ID, engine)



# import os
# import requests
# from bs4 import BeautifulSoup
# from dotenv import load_dotenv
# import sqlalchemy
# from auto_loader import auto_loader
# from netCDF4 import Dataset

# # Load environment variables
# load_dotenv()
# DB_URL = os.getenv("DB_URL")
# engine = sqlalchemy.create_engine(DB_URL, pool_pre_ping=True, future=True)

# BASE_URL = "https://data-argo.ifremer.fr/dac/incois/"

# def get_float_ids():
#     """Fetch Float IDs from directory listing"""
#     response = requests.get(BASE_URL)
#     soup = BeautifulSoup(response.text, "html.parser")

#     float_ids = []
#     for link in soup.find_all("a"):
#         text = link.get("href").strip("/")
#         if text.isdigit():  # FILTER only numeric folder names
#             float_ids.append(text)

#     return float_ids

# def get_float_date(float_id):
#     """Read metadata file (.nc) and extract deployment date"""
#     meta_url = f"{BASE_URL}{float_id}/{float_id}_meta.nc"

#     try:
#         dataset = Dataset(meta_url)
#         if "DATE_CREATION" in dataset.variables:
#             date_str = "".join([chr(i) for i in dataset["DATE_CREATION"][:]]).strip()
#             return date_str[:7]  # format YYYY-MM
#         return None
#     except:
#         return None

# def auto_run():
#     """Main logic"""
#     print("🔍 Fetching float list...")
#     float_ids = get_float_ids()

#     print(f"📌 {len(float_ids)} floats found")

#     target_month = "2025-11"
#     selected = []

#     for f_id in float_ids:
#         date = get_float_date(f_id)
#         if date == target_month:
#             selected.append(f_id)

#     print(f"🎯 Found {len(selected)} floats matching {target_month}: {selected}")

#     for float_id in selected:
#         print(f"⚙ Running auto_loader for {float_id} ...")
#         auto_loader(float_id, engine)

#     print("\n✅ All processing complete!")

# # Run
# auto_run()

# FLOAT_ID = [4903837, 5907140, 5907176, 6990612, 7901130, 7902249, 6990715, 7902244, 2902224, 2902295, 2903894, 2903895, 2903988, 3902573, 3902629, 6990715]

# for selected in FLOAT_ID:
#          print(f"⚙ Running auto_loader for {selected} ...")
#          auto_loader(selected, engine)