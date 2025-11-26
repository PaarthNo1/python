# db_insert.py

from sqlalchemy import text
import numpy as np


def _clean_val(v):
    """Convert numpy scalar values to native Python types."""
    if isinstance(v, np.generic):
        return v.item()
    return v


def insert_float_metadata(engine, data):
    """
    Upsert a single floats row using data dict returned by parser.
    - Upsert on (float_id, cycle)
    - Prefer new metadata if provided (COALESCE)
    - Convert numpy + nanosecond timestamps safely
    """

    # Validate required fields
    float_id = data.get("float_id")
    cycle = data.get("cycle")
    if float_id is None or cycle is None:
        raise ValueError("float_id and cycle are required for insert")

    # Clean and convert values (especially numpy types)
    latitude = _clean_val(data.get("latitude"))
    longitude = _clean_val(data.get("longitude"))

    # Round timestamp to microseconds to avoid Postgres errors
    juld = data.get("juld")
    if juld is not None and hasattr(juld, "round"):
        try:
            juld = juld.round("us")
        except Exception:
            pass

    # Prepare parameters
    params = {
        "float_id": float_id,
        "cycle": int(cycle),
        "profile_number": data.get("profile_number"),
        "latitude": latitude,
        "longitude": longitude,
        "juld": juld,
        "source_file": data.get("source_file"),
        "wmo_id": _clean_val(data.get("wmo_id")),
        "platform_type": _clean_val(data.get("platform_type")),
        "project_name": _clean_val(data.get("project_name")),
        "pi_name": _clean_val(data.get("pi_name")),
    }

    # Build GEOM expression depending on lat/lon presence
    if latitude is not None and longitude is not None:
        geom_expr = "ST_SetSRID(ST_MakePoint(:longitude, :latitude), 4326)"
    else:
        geom_expr = "NULL"

    insert_sql = f"""
        INSERT INTO floats (
            float_id, cycle, profile_number,
            wmo_id, platform_type, project_name, pi_name,
            latitude, longitude, juld, source_file, geom
        )
        VALUES (
            :float_id, :cycle, :profile_number,
            :wmo_id, :platform_type, :project_name, :pi_name,
            :latitude, :longitude, :juld, :source_file, {geom_expr}
        )
        ON CONFLICT (float_id, cycle)
        DO UPDATE SET
            profile_number = EXCLUDED.profile_number,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            juld = EXCLUDED.juld,
            source_file = EXCLUDED.source_file,
            wmo_id = COALESCE(EXCLUDED.wmo_id, floats.wmo_id),
            platform_type = COALESCE(EXCLUDED.platform_type, floats.platform_type),
            project_name = COALESCE(EXCLUDED.project_name, floats.project_name),
            pi_name = COALESCE(EXCLUDED.pi_name, floats.pi_name),
            geom = EXCLUDED.geom;
    """

    # Execute inside a transaction
    with engine.begin() as conn:
        conn.execute(text(insert_sql), params)

    print(f"✔ Upserted floats ({float_id}, cycle={cycle})")
