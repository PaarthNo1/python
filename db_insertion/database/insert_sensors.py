# insert_sensors.py
import json
from sqlalchemy import text


def insert_sensors(conn, float_id, sensors):
    """
    Insert / update sensor definitions in sensors_catalog.
    - Accepts float_id (ignored, for compatibility).
    - Uses passed connection (no new transaction).
    """

    # Guard: nothing to do
    if not sensors:
        return

    # Prepare clean rows (JSON-encode calibration_meta)
    clean_rows = []
    for s in sensors:
        s_copy = s.copy()
        s_copy["calibration_meta"] = json.dumps(s_copy.get("calibration_meta") or {})
        clean_rows.append(s_copy)

    insert_sql = text("""
        INSERT INTO sensors_catalog (
            sensor_name, model, manufacturer, units, description, calibration_meta
        )
        VALUES (
            :sensor_name, :model, :manufacturer, :units, :description, :calibration_meta
        )
        ON CONFLICT (sensor_name) DO UPDATE
           SET model = COALESCE(EXCLUDED.model, sensors_catalog.model),
               manufacturer = COALESCE(EXCLUDED.manufacturer, sensors_catalog.manufacturer),
               units = COALESCE(EXCLUDED.units, sensors_catalog.units),
               description = COALESCE(EXCLUDED.description, sensors_catalog.description),
               calibration_meta = COALESCE(EXCLUDED.calibration_meta, sensors_catalog.calibration_meta);
    """)

    # Execute using passed connection
    conn.execute(insert_sql, clean_rows)

    print(f"✔ Inserted/Updated {len(sensors)} sensors into sensors_catalog (stable mode)")
