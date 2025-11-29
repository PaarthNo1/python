
# smart_sensor_parser_v2.py  (ULTRA OPTIMIZED)
import xarray as xr
import numpy as np
import json
from dataset_cache import CACHE

def clean_bytes(x):
    if isinstance(x, (bytes, np.bytes_)):
        return x.decode("utf-8", errors="ignore").strip()
    if isinstance(x, (list, np.ndarray)):
        out = []
        for v in x:
            if isinstance(v, (bytes, np.bytes_)):
                out.append(v.decode("utf-8", errors="ignore").strip())
            else:
                out.append(str(v).strip())
        return "".join(out).strip()
    return str(x).strip()

def extract_float_id(raw):
    """
    Convert PLATFORM_NUMBER like [b'1', b'9', b'0', ...] → "1900042"
    """
    if raw is None:
        return None

    # case: array of bytes
    if isinstance(raw, (np.ndarray, list)):
        s = ""
        for v in raw:
            if isinstance(v, (bytes, np.bytes_)):
                v = v.decode("utf-8", errors="ignore")
            v = str(v).strip()
            if v and v not in ("\x00", " "):
                s += v
        return s.strip()

    # single value
    if isinstance(raw, (bytes, np.bytes_)):
        return raw.decode("utf-8", errors="ignore").strip()

    return str(raw).strip()


# ----------------------------------------------------------
# FAST + SAFE CHAR DECODER
# ----------------------------------------------------------
def decode_char_array(arr):
    """Fast and safe conversion of NetCDF byte/char arrays."""
    if arr is None:
        return None
    try:
        flat = arr.flatten()
    except Exception:
        flat = arr

    out = []
    for c in flat:
        if isinstance(c, (bytes, np.bytes_)):
            s = c.decode("utf-8", errors="ignore").strip()
            if s:
                out.append(s)
        else:
            s = str(c).strip()
            if s not in ("", "0", "None"):
                out.append(s)

    return "".join(out).strip() if out else None


def safe_get_attr(attrs, *keys):
    """Return first available attribute name from keys."""
    for k in keys:
        if k in attrs:
            return attrs[k]
    return None


# ----------------------------------------------------------
# DEFAULT SENSOR DEFINITIONS (used for fallback)
# ----------------------------------------------------------
DEFAULT_MAP = {
    "PRES":  {"model":"SBE41CP","manufacturer":"Sea-Bird","units":"dbar","description":"Sea water pressure (0 at sea surface)"},
    "TEMP":  {"model":"SBE41CP","manufacturer":"Sea-Bird","units":"degree_Celsius","description":"Sea temperature (ITS-90)"},
    "PSAL":  {"model":"SBE41CP","manufacturer":"Sea-Bird","units":"PSU","description":"Practical salinity (PSS-78)"},
    "DOXY":  {"model":"Aanderaa 4330","manufacturer":"Aanderaa","units":"umol/kg","description":"Dissolved oxygen (optode)"},
    "CHLA":  {"model":"WetLabs FLBB","manufacturer":"WetLabs","units":"mg m-3","description":"Chlorophyll-a fluorescence"},
    "NITRATE":{"model":"SUNA","manufacturer":"SUNA","units":"umol/kg","description":"Nitrate concentration"},
    "BBP":   {"model":"WetLabs","manufacturer":"WetLabs","units":"1 m-1","description":"Backscattering"},
    "PH":    {"model":None,"manufacturer":None,"units":"pH","description":"pH (total)"},
    "CNDC":  {"model":"SBE41CP","manufacturer":"Sea-Bird","units":"mS cm-1","description":"Conductivity"}
}

WHITELIST_BASES = set(DEFAULT_MAP.keys()) | {
    "TEMP","PSAL","PRES","DOXY","CHLA","NITRATE",
    "PH","BBP","CNDC","CDOM","BB","FLUOR"
}


# ----------------------------------------------------------
# MAIN PARSER (MAX SPEED)
# ----------------------------------------------------------
def parse_sensors_hybrid(profile_url, meta_url, tech_url, smart_fill=True):
    """
    Combined parser:
      - read profile first (main sensor source)
      - read tech (model detection)
      - read meta (calibration blocks)
    Returns a list of sensor dicts.
    """

    # ------------------------------------------------------
    # 1) OPEN **ALL FILES ONCE** (BIG SPEED BOOST)
    # ------------------------------------------------------
    ds_prof = CACHE.get_dataset(profile_url, decode_cf=False, mask_and_scale=False, decode_times=False)
    ds_tech = CACHE.get_dataset(tech_url, decode_cf=False, mask_and_scale=False, decode_times=False)
    ds_meta = CACHE.get_dataset(meta_url, decode_cf=False, mask_and_scale=False, decode_times=False)


    sensors = {}  # key = base sensor name (TEMP, PSAL ...)


    # ------------------------------------------------------
    # 2) PROFILE: Extract units + description
    # ------------------------------------------------------
    for var in ds_prof.variables:
        name = var.upper()
        base = name.split("_")[0]

        if base not in WHITELIST_BASES:
            continue

        if name in ("JULD","LATITUDE","LONGITUDE","PLATFORM_NUMBER","CYCLE_NUMBER","PROFILE_NUMBER"):
            continue

        attrs = ds_prof[var].attrs or {}

        units = safe_get_attr(attrs, "units", "UNIT", "PARAMETER_UNITS")
        long_name = safe_get_attr(attrs, "long_name", "standard_name", "longName")

        if base not in sensors:
            sensors[base] = {
                "sensor_name": base,
                "model": None,
                "manufacturer": None,
                "units": decode_char_array(units) if isinstance(units, (np.ndarray,bytes)) else (units or None),
                "description": decode_char_array(long_name) if isinstance(long_name, (np.ndarray,bytes)) else (long_name or None),
                "calibration_meta": {}
            }


    # ------------------------------------------------------
    # 3) TECH: auto-detect models (SBE, SUNA, AANDERAA, etc.)
    # ------------------------------------------------------
    tech_models = []
    if ("TECHNICAL_PARAMETER_NAME" in ds_tech and
        "TECHNICAL_PARAMETER_VALUE" in ds_tech):

        names = ds_tech["TECHNICAL_PARAMETER_NAME"].values
        values = ds_tech["TECHNICAL_PARAMETER_VALUE"].values

        for i in range(min(len(names), len(values))):
            n = decode_char_array(names[i])
            v = decode_char_array(values[i])
            if n and v:
                tech_models.append((n, v))

    # assign models
    for base in list(sensors.keys()):
        for pname, pval in tech_models:
            up = pval.upper() if pval else ""
            if any(tag in up for tag in ["SBE","AAND","4330","SUNA","WETLAB","FL"]):
                if not sensors[base]["model"]:
                    sensors[base]["model"] = pval
                    # manufacturer guess
                    if "SBE" in up:
                        sensors[base]["manufacturer"] = "Sea-Bird"
                    elif "AAND" in up or "4330" in up:
                        sensors[base]["manufacturer"] = "Aanderaa"
                    elif "WET" in up or "FL" in up:
                        sensors[base]["manufacturer"] = "WetLabs"


    # ------------------------------------------------------
    # 4) META: extract calibration blocks
    # ------------------------------------------------------
    calibration_blocks = []
    for var in ds_meta.variables:
        attrs = ds_meta[var].attrs or {}

        cal_date = safe_get_attr(attrs, "CALIBRATION_DATE", "calibration_date")
        cal_coeff = safe_get_attr(attrs, "CALIBRATION_COEFFICIENT", "CALIBRATION_COEFFICIENTS")

        if cal_date or cal_coeff:
            calibration_blocks.append({
                "variable": var,
                "calibration_date": decode_char_array(cal_date) if isinstance(cal_date,(np.ndarray,bytes)) else cal_date,
                "calibration_coefficients": decode_char_array(cal_coeff) if isinstance(cal_coeff,(np.ndarray,bytes)) else cal_coeff
            })

    # assign calibration
    if calibration_blocks:
        attached = False
        for block in calibration_blocks:
            vname = str(block["variable"]).upper()
            for base in sensors:
                if base in vname:
                    sensors[base]["calibration_meta"] = [block]
                    attached = True
        if not attached:
            for base in sensors:
                sensors[base]["calibration_meta"] = calibration_blocks


    # ------------------------------------------------------
    # 5) SMART FILL MISSING VALUES (DEFAULT_MAP)
    # ------------------------------------------------------
    if smart_fill:
        for base, entry in sensors.items():
            if base in DEFAULT_MAP:
                dm = DEFAULT_MAP[base]

                if not entry["units"]:
                    entry["units"] = dm.get("units")
                if not entry["model"]:
                    entry["model"] = dm.get("model")
                if not entry["manufacturer"]:
                    entry["manufacturer"] = dm.get("manufacturer")
                if not entry["description"]:
                    entry["description"] = dm.get("description")


    # ------------------------------------------------------
    # 6) MAKE JSON SAFE + convert to list
    # ------------------------------------------------------
    out = []
    for entry in sensors.values():
        try:
            json.dumps(entry["calibration_meta"])
        except:
            entry["calibration_meta"] = {"note":"unserializable"}
        out.append(entry)

    print(f"✔ Smart sensors parser v2: {len(out)} sensors extracted/enriched")
    return out
