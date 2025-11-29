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

def get_db_stats():
    print("\n📊 Database Statistics\n" + "="*30)
    
    with engine.connect() as conn:
        # 1. Table Row Counts
        print("\n🔢 Row Counts:")
        tables = ["floats", "profiles", "measurements", "traj", "tech", "meta_kv"]
        for table in tables:
            try:
                count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
                print(f"   - {table.ljust(15)}: {count:,} rows")
            except Exception as e:
                print(f"   - {table.ljust(15)}: ⚠ Error (Table might not exist)")

        # 2. Database Size
        print("\n💾 Storage Size:")
        try:
            # Total DB size
            db_name = engine.url.database
            size_sql = text("SELECT pg_size_pretty(pg_database_size(:db))")
            total_size = conn.execute(size_sql, {"db": db_name}).scalar()
            print(f"   - Total Database : {total_size}")

            # Top 5 Largest Tables
            print("\n📉 Largest Tables:")
            table_size_sql = text("""
                SELECT
                    relname AS table_name,
                    pg_size_pretty(pg_total_relation_size(relid)) AS total_size
                FROM pg_catalog.pg_statio_user_tables
                ORDER BY pg_total_relation_size(relid) DESC
                LIMIT 5;
            """)
            rows = conn.execute(table_size_sql).fetchall()
            for row in rows:
                print(f"   - {row[0].ljust(15)}: {row[1]}")
                
        except Exception as e:
            print(f"⚠ Could not fetch size info: {e}")

    print("\n" + "="*30 + "\n")

if __name__ == "__main__":
    get_db_stats()
