# fallbacks.py
from typing import Dict, Any

def fallback_sql_for_common_patterns(question: str) -> Dict[str, Any]:
    import re
    coords = re.findall(r"(-?\d+\.\d+)", question)
    if len(coords) >= 2:
        try:
            lat = float(coords[0]); lon = float(coords[1])
        except Exception:
            lat = None; lon = None
    else:
        lat = None; lon = None

    if lat is not None and lon is not None:
        sql = """
SELECT f.float_id, f.cycle, f.profile_number, f.lat, f.lon, f.juld,
       MAX(m.temp) FILTER (WHERE m.depth < :p_depth) AS max_surface_temp
FROM floats f
JOIN measurements m ON f.float_id = m.float_id AND f.cycle = m.cycle AND f.profile_number = m.profile_number
WHERE f.lat BETWEEN :p_lat_min AND :p_lat_max
  AND f.lon BETWEEN :p_lon_min AND :p_lon_max
GROUP BY f.float_id, f.cycle, f.profile_number, f.lat, f.lon, f.juld
ORDER BY max_surface_temp DESC
LIMIT :p0
"""
        params = {
            "p0": 50,
            "p_depth": 10,
            "p_lat_min": lat - 2.0,
            "p_lat_max": lat + 2.0,
            "p_lon_min": lon - 2.0,
            "p_lon_max": lon + 2.0,
        }
        return {"sql": sql, "params": params, "explain": "Fallback: max surface temp per profile in bounding box"}

    sql = """
SELECT f.float_id, f.cycle, f.profile_number, f.lat, f.lon, f.juld,
       MAX(m.temp) FILTER (WHERE m.depth < :p_depth) AS max_surface_temp
FROM floats f
JOIN measurements m ON f.float_id = m.float_id AND f.cycle = m.cycle AND f.profile_number = m.profile_number
WHERE m.temp IS NOT NULL
GROUP BY f.float_id, f.cycle, f.profile_number, f.lat, f.lon, f.juld
ORDER BY juld DESC
LIMIT :p0
"""
    params = {"p0": 50, "p_depth": 10}
    return {"sql": sql, "params": params, "explain": "Fallback: recent profiles with surface temp"}
