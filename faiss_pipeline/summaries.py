# summaries.py
import pandas as pd
from typing import Any
import logging

logger = logging.getLogger("faiss.summaries")

def build_summary(row: pd.Series) -> str:
    float_id = str(row["float_id"])
    cycle = int(row["cycle"])
    prof = int(row["profile_number"])
    lat = float(row["lat"]) if pd.notna(row["lat"]) else None
    lon = float(row["lon"]) if pd.notna(row["lon"]) else None
    juld = row["juld"]
    n_points = int(row["n_points"]) if pd.notna(row["n_points"]) else 0
    mean_temp = float(row["mean_temp"]) if pd.notna(row["mean_temp"]) else None
    mean_sal = float(row["mean_sal"]) if pd.notna(row["mean_sal"]) else None
    min_d = float(row["min_depth"]) if pd.notna(row["min_depth"]) else None
    max_d = float(row["max_depth"]) if pd.notna(row["max_depth"]) else None

    parts = [f"Float {float_id}, cycle {cycle} (profile {prof})."]
    if juld is not None:
        parts.append(f"Date: {pd.to_datetime(juld).strftime('%Y-%m-%d')}.")
    if lat is not None and lon is not None:
        parts.append(f"Location: {lat:.3f}N, {lon:.3f}E.")
    if n_points:
        parts.append(f"{n_points} depth levels from {min_d:.1f}m to {max_d:.1f}m.")
    if mean_temp is not None:
        parts.append(f"Mean temperature: {mean_temp:.2f} °C.")
    if mean_sal is not None:
        parts.append(f"Mean salinity: {mean_sal:.2f} PSU.")
    return " ".join(parts)
