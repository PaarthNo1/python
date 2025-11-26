# insert_sensors.py

from psycopg2.extras import execute_batch
from psycopg2 import OperationalError
from sqlalchemy import text
import json

def insert_sensors(engine, sensors):
    clean_rows = []
    for s in sensors:
        s_copy = s.copy()
        s_copy["calibration_meta"] = json.dumps(s_copy.get("calibration_meta") or {})
        clean_rows.append(s_copy)

    insert_sql = """
        INSERT INTO sensors_catalog (
            sensor_name, model, manufacturer, units, description, calibration_meta
        )
        VALUES (
            %(sensor_name)s, %(model)s, %(manufacturer)s, %(units)s, %(description)s, %(calibration_meta)s
        )
        ON CONFLICT (sensor_name) DO UPDATE
            SET model = COALESCE(EXCLUDED.model, sensors_catalog.model),
                manufacturer = COALESCE(EXCLUDED.manufacturer, sensors_catalog.manufacturer),
                units = COALESCE(EXCLUDED.units, sensors_catalog.units),
                description = COALESCE(EXCLUDED.description, sensors_catalog.description),
                calibration_meta = COALESCE(EXCLUDED.calibration_meta, sensors_catalog.calibration_meta);
    """

    # Use fresh DB connection for safety
    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()

        try:
            # smaller batch size = safer + faster
            execute_batch(cur, insert_sql, clean_rows, page_size=50)
            raw_conn.commit()

        except OperationalError:
            print("⚠ Batch insert failed → using slow fallback mode")

            raw_conn.rollback()
            for row in clean_rows:
                try:
                    cur.execute(insert_sql, row)
                except Exception as e:
                    print("❌ Single-row insert failed:", e)
            raw_conn.commit()

    finally:
        raw_conn.close()

    print(f"✔ Inserted/Updated {len(sensors)} sensors (safe mode)")
