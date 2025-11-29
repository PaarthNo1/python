import os
import sqlalchemy
from sqlalchemy import text
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DB_URL")
engine = sqlalchemy.create_engine(DB_URL)

def check_summary_view():
    print("🔍 Checking float_summary_mv data...\n")
    sql = text("SELECT * FROM float_summary_mv LIMIT 10")
    
    try:
        with engine.connect() as conn:
            rows = conn.execute(sql).fetchall()
            
        if not rows:
            print("⚠ View is empty. Try running REFRESH MATERIALIZED VIEW.")
            return

        print(f"{'Float ID':<10} | {'Cycle':<5} | {'Date':<20} | {'Profiles':<8} | {'Lat':<8} | {'Lon':<8} | {'Status'}")
        print("-" * 90)
        
        for row in rows:
            # Handle potential None values safely
            fid = row[0]
            cycle = row[1] if row[1] is not None else -1
            date = str(row[2])[:19] if row[2] else "N/A"
            count = row[3]
            lat = f"{row[4]:.2f}" if row[4] is not None else "N/A"
            lon = f"{row[5]:.2f}" if row[5] is not None else "N/A"
            status = row[6]
            
            print(f"{fid:<10} | {cycle:<5} | {date:<20} | {count:<8} | {lat:<8} | {lon:<8} | {status}")
            
    except Exception as e:
        print(f"❌ Error querying view: {e}")
        print("💡 Tip: Ensure the view exists. Run optimize_db.py if needed.")

if __name__ == "__main__":
    check_summary_view()
