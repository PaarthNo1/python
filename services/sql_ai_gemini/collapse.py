# collapse.py
from typing import List, Dict, Any, Any as AnyT

def collapse_rows_to_profiles(rows: List[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
    if not rows:
        return []

    profiles: Dict[AnyT, Dict[str, Any]] = {}
    for r in rows:
        t = r.get("temp")
        if t is None:
            continue
        key = (r.get("float_id"), int(r.get("cycle")) if r.get("cycle") is not None else None)
        try:
            temp_val = float(t)
        except Exception:
            continue

        cur = profiles.get(key)
        if cur is None or temp_val > cur["max_surface_temp"]:
            profiles[key] = {
                "float_id": r.get("float_id"),
                "cycle": int(r.get("cycle")) if r.get("cycle") is not None else None,
                "profile_number": r.get("profile_number"),
                "lat": r.get("lat"),
                "lon": r.get("lon"),
                "juld": r.get("juld"),
                "max_surface_temp": temp_val,
                "depth_at_max": r.get("depth"),
            }

    out = list(profiles.values())
    out.sort(key=lambda x: x.get("max_surface_temp", -9999), reverse=True)
    return out[:int(limit)]
