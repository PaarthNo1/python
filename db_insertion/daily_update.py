import os
import time
import sqlalchemy
from sqlalchemy import text
from dotenv import load_dotenv
from auto_loader import auto_loader

# Load environment variables
load_dotenv()

# DB Connection
DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise ValueError("❌ DB_URL not found in .env file.")

engine = sqlalchemy.create_engine(
    DB_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    pool_timeout=30,
    future=True
)

def get_active_floats():
    """
    Fetch list of ALL floats currently in the database.
    We want to update existing floats with new profiles.
    """
    print("🔍 Fetching active floats from database...")
    sql = text("SELECT DISTINCT float_id FROM floats")
    with engine.begin() as conn:
        rows = conn.execute(sql).fetchall()
    
    float_ids = [row[0] for row in rows]
    print(f"📌 Found {len(float_ids)} active floats.")
    return float_ids

def refresh_summary_view():
    """
    Refresh the Materialized View to update dashboard stats.
    """
    print("\n🔄 Refreshing Summary Materialized View...")
    start = time.time()
    try:
        with engine.begin() as conn:
            conn.execute(text("REFRESH MATERIALIZED VIEW CONCURRENTLY float_summary_mv"))
        print(f"✅ Summary View Refreshed in {time.time() - start:.2f}s")
    except Exception as e:
        print(f"⚠ Failed to refresh view (might be locked or not concurrent): {e}")
        # Fallback to non-concurrent if unique index is missing
        try:
            with engine.begin() as conn:
                conn.execute(text("REFRESH MATERIALIZED VIEW float_summary_mv"))
            print(f"✅ Summary View Refreshed (Non-Concurrent) in {time.time() - start:.2f}s")
        except Exception as e2:
             print(f"❌ Failed to refresh view: {e2}")

def main():
    print("🚀 Starting Daily Update Job...")
    start_time = time.time()
    
    # 1. Get Floats to Update
    # For now, we update ALL floats in the DB.
    # In future, we can add logic to discover NEW floats here.
    floats = get_active_floats()
    
    # 2. Run Auto Loader for each float
    for i, float_id in enumerate(floats):
        print(f"\n[{i+1}/{len(floats)}] Updating Float {float_id}...")
        try:
            auto_loader(float_id, engine)
        except Exception as e:
            print(f"❌ Error updating {float_id}: {e}")
            
    # 3. Refresh Database Stats
    refresh_summary_view()
    
    print(f"\n🎉 Daily Update Completed in {time.time() - start_time:.2f}s")

if __name__ == "__main__":
    main()
