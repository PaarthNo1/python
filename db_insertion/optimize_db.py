import os
from dotenv import load_dotenv
import sqlalchemy
from sqlalchemy import text

# Load environment variables
load_dotenv()
DB_URL = os.getenv("DB_URL")

if not DB_URL:
    print("❌ DB_URL not found in .env")
    exit(1)

engine = sqlalchemy.create_engine(DB_URL)

def optimize_database():
    print("\n🚀 Starting Database Optimization...\n" + "="*40)
    
    with engine.begin() as conn:
        # ---------------------------------------------------------
        # 1. Spatial Indexing (GIST) for Fast Maps
        # ---------------------------------------------------------
        print("\n🌍 Applying Spatial Indices (GIST)...")
        
        tables_with_geom = ["profiles", "traj", "floats"]
        
        for table in tables_with_geom:
            try:
                # Check if table exists first
                check_table = text(f"SELECT to_regclass('public.{table}')")
                if conn.execute(check_table).scalar():
                    index_name = f"idx_{table}_geom"
                    sql = text(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table} USING GIST (geom);")
                    conn.execute(sql)
                    print(f"   ✔ Index '{index_name}' created/verified on '{table}'.")
                else:
                    print(f"   ⚠ Table '{table}' does not exist, skipping index.")
            except Exception as e:
                print(f"   ❌ Failed to index {table}: {e}")

        # ---------------------------------------------------------
        # 2. Summary Table (Materialized View) for Fast Dashboard
        # ---------------------------------------------------------
        print("\n📊 Creating Summary Materialized View...")
        
        mv_name = "float_summary_mv"
        
        # Drop if exists to ensure fresh definition (optional, but good for updates)
        # conn.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name};")) 
        # Better to use CREATE MATERIALIZED VIEW IF NOT EXISTS, but if logic changes we might need to drop.
        # For now, let's assume we want to create it if missing.
        
        mv_sql = text(f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {mv_name} AS
        SELECT 
            f.float_id,
            MAX(p.cycle) as last_cycle,
            MAX(p.juld) as last_profile_date,
            COUNT(p.profile_number) as num_profiles,
            
            -- Get location of the last profile (by date)
            (ARRAY_AGG(p.lat ORDER BY p.juld DESC))[1] as last_lat,
            (ARRAY_AGG(p.lon ORDER BY p.juld DESC))[1] as last_lon,
            
            -- Simple status logic
            CASE 
                WHEN MAX(p.juld) > NOW() - INTERVAL '30 days' THEN 'Active'
                ELSE 'Inactive'
            END as status
            
        FROM floats f
        LEFT JOIN profiles p ON f.float_id = p.float_id
        GROUP BY f.float_id;
        """)
        
        try:
            conn.execute(mv_sql)
            print(f"   ✔ Materialized View '{mv_name}' created/verified.")
            
            # Create Unique Index for Concurrent Refresh
            idx_sql = text(f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{mv_name}_fid ON {mv_name} (float_id);")
            conn.execute(idx_sql)
            print(f"   ✔ Unique Index created on '{mv_name}' (enables CONCURRENT REFRESH).")
            
        except Exception as e:
            print(f"   ❌ Failed to create Materialized View: {e}")

    # ---------------------------------------------------------
    # 3. Refresh Data
    # ---------------------------------------------------------
    print("\n🔄 Refreshing Summary Data...")
    try:
        # Refresh must be outside transaction block for concurrent, or inside for normal.
        # Since we used engine.begin(), we are in a transaction. 
        # REFRESH MATERIALIZED VIEW cannot run inside a transaction block if CONCURRENTLY is used?
        # Actually standard REFRESH is fine.
        with engine.connect() as conn:
             conn.execute(text(f"REFRESH MATERIALIZED VIEW {mv_name};"))
        print(f"   ✔ Data refreshed successfully.")
    except Exception as e:
        print(f"   ⚠ Refresh failed (might need manual refresh): {e}")

    print("\n" + "="*40 + "\n✅ Optimization Complete!")

if __name__ == "__main__":
    optimize_database()
