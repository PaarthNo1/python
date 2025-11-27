# sql_patterns.py
PATTERNS = [
  # A0) Top-N hottest profiles on a given day (arrays) — viz-friendly points
  {
    "title": "top-N hottest profiles on date (arrays, viz)",
    "sql": """\
WITH per_profile AS (
  SELECT
    p.float_id, p.cycle, p.profile_number,
    p.lat AS lat, p.lon AS lon, p.juld AS juld,
    MAX(u.temp) AS max_temperature
  FROM profiles p
  LEFT JOIN LATERAL unnest(p.temp) AS u(temp) ON TRUE
  WHERE p.juld >= :p5 AND p.juld < :p6
  GROUP BY p.float_id, p.cycle, p.profile_number, p.lat, p.lon, p.juld
)
SELECT float_id, cycle, profile_number, lat, lon, juld, max_temperature
FROM per_profile
ORDER BY max_temperature DESC NULLS LAST
LIMIT :p0"""
  },

  # A1) Top-N hottest profiles in region + day (arrays)
  {
    "title": "top-N hottest profiles in region on date (arrays, viz)",
    "sql": """\
WITH per_profile AS (
  SELECT
    p.float_id, p.cycle, p.profile_number,
    p.lat AS lat, p.lon AS lon, p.juld AS juld,
    MAX(u.temp) AS max_temperature
  FROM profiles p
  LEFT JOIN LATERAL unnest(p.temp) AS u(temp) ON TRUE
  WHERE p.lon BETWEEN :p1 AND :p2
    AND p.lat BETWEEN :p3 AND :p4
    AND p.juld >= :p5 AND p.juld < :p6
  GROUP BY p.float_id, p.cycle, p.profile_number, p.lat, p.lon, p.juld
)
SELECT float_id, cycle, profile_number, lat, lon, juld, max_temperature
FROM per_profile
ORDER BY max_temperature DESC NULLS LAST
LIMIT :p0"""
  },

  # A2) Depth–Temp–Sal for a specific profile (arrays)
  {
    "title": "profile depth-temp-sal with context (arrays, viz)",
    "sql": """\
WITH pts AS (
  SELECT
    p.float_id, p.cycle, p.profile_number,
    p.lat AS lat, p.lon AS lon, p.juld AS juld,
    u.pres AS depth_m, u.temp AS temperature, u.psal AS salinity
  FROM profiles p
  LEFT JOIN LATERAL unnest(p.pres, p.temp, p.psal) AS u(pres, temp, psal) ON TRUE
  WHERE p.float_id = :p5 AND p.cycle = :p6
)
SELECT float_id, cycle, profile_number, lat, lon, juld, depth_m, temperature, salinity
FROM pts
ORDER BY depth_m
LIMIT :p0"""
  },

  # A3) Profiles in lon/lat box
  {
    "title": "profiles in lon/lat box (viz)",
    "sql": """\
SELECT float_id, cycle, profile_number, lat, lon, juld
FROM profiles
WHERE lon BETWEEN :p1 AND :p2 AND lat BETWEEN :p3 AND :p4
ORDER BY juld DESC
LIMIT :p0"""
  },

  # A4) Profiles in time window
  {
    "title": "profiles in time window (viz)",
    "sql": """\
SELECT float_id, cycle, profile_number, lat, lon, juld
FROM profiles
WHERE juld >= :p5 AND juld < :p6
ORDER BY juld
LIMIT :p0"""
  },

  # A5) Highest salinity values per profile (arrays)
  {
    "title": "top-N highest salinity per profile (arrays, viz)",
    "sql": """\
WITH per_profile AS (
  SELECT
    p.float_id, p.cycle, p.profile_number,
    p.lat AS lat, p.lon AS lon, p.juld AS juld,
    MAX(u.psal) AS max_salinity
  FROM profiles p
  LEFT JOIN LATERAL unnest(p.psal) AS u(psal) ON TRUE
  WHERE p.float_id = :p5 AND p.cycle = :p6
  GROUP BY p.float_id, p.cycle, p.profile_number, p.lat, p.lon, p.juld
)
SELECT float_id, cycle, profile_number, lat, lon, juld, max_salinity
FROM per_profile
ORDER BY max_salinity DESC NULLS LAST
LIMIT :p0"""
  },

  # A6) Mean temp/sal per profile in time window (arrays)
  {
    "title": "mean temp/sal per profile in time window (arrays, viz)",
    "sql": """\
WITH flat AS (
  SELECT p.float_id, p.cycle, p.profile_number, p.lat, p.lon, p.juld, u.temp, u.psal
  FROM profiles p
  LEFT JOIN LATERAL unnest(p.temp, p.psal) AS u(temp, psal) ON TRUE
  WHERE p.juld >= :p5 AND p.juld < :p6
),
agg AS (
  SELECT
    float_id, cycle, profile_number,
    MIN(lat) AS lat, MIN(lon) AS lon, MIN(juld) AS juld,
    AVG(temp) AS mean_temperature,
    AVG(psal) AS mean_salinity
  FROM flat
  GROUP BY float_id, cycle, profile_number
)
SELECT float_id, cycle, profile_number, lat, lon, juld, mean_temperature, mean_salinity
FROM agg
ORDER BY juld DESC
LIMIT :p0"""
  },

  # A7) Max temp in arbitrary window (arrays)
  {
    "title": "max temp per profile in window (arrays, viz)",
    "sql": """\
WITH per_profile AS (
  SELECT
    p.float_id, p.cycle, p.profile_number, p.lat, p.lon, p.juld,
    MAX(u.temp) AS max_temperature
  FROM profiles p
  LEFT JOIN LATERAL unnest(p.temp) AS u(temp) ON TRUE
  WHERE p.juld >= :p5 AND p.juld < :p6
  GROUP BY p.float_id, p.cycle, p.profile_number, p.lat, p.lon, p.juld
)
SELECT float_id, cycle, profile_number, lat, lon, juld, max_temperature
FROM per_profile
ORDER BY max_temperature DESC NULLS LAST
LIMIT :p0"""
  },

  # B0) Top-N hottest profiles via measurements
  {
    "title": "top-N hottest profiles on date via measurements (viz)",
    "sql": """\
WITH per_profile AS (
  SELECT
    f.float_id, f.cycle, f.profile_number,
    f.latitude AS lat, f.longitude AS lon, m.juld AS juld,
    MAX(m.value) AS max_temperature
  FROM floats f
  JOIN measurements m
    ON f.float_id = m.float_id
   AND f.cycle = m.cycle
   AND f.profile_number = m.profile_number
  WHERE lower(m.sensor) LIKE 'temp%%'
    AND m.juld >= :p5 AND m.juld < :p6
  GROUP BY f.float_id, f.cycle, f.profile_number, f.latitude, f.longitude, m.juld
)
SELECT float_id, cycle, profile_number, lat, lon, juld, max_temperature
FROM per_profile
ORDER BY max_temperature DESC NULLS LAST
LIMIT :p0"""
  },

  # B1) Region + date via measurements
  {
    "title": "top-N hottest profiles in region on date via measurements (viz)",
    "sql": """\
WITH per_profile AS (
  SELECT
    f.float_id, f.cycle, f.profile_number,
    f.latitude AS lat, f.longitude AS lon, m.juld AS juld,
    MAX(m.value) AS max_temperature
  FROM floats f
  JOIN measurements m
    ON f.float_id = m.float_id
   AND f.cycle = m.cycle
   AND f.profile_number = m.profile_number
  WHERE lower(m.sensor) LIKE 'temp%%'
    AND f.longitude BETWEEN :p1 AND :p2
    AND f.latitude  BETWEEN :p3 AND :p4
    AND m.juld >= :p5 AND m.juld < :p6
  GROUP BY f.float_id, f.cycle, f.profile_number, f.latitude, f.longitude, m.juld
)
SELECT float_id, cycle, profile_number, lat, lon, juld, max_temperature
FROM per_profile
ORDER BY max_temperature DESC NULLS LAST
LIMIT :p0"""
  },

  # B2) Depth pivot (measurements)
  {
    "title": "measurements pivot depth temp/sal with context (viz)",
    "sql": """\
WITH pivoted AS (
  SELECT
    m.depth_m AS depth_m,
    MAX(CASE WHEN m.sensor='temp' THEN m.value END) AS temperature,
    MAX(CASE WHEN m.sensor='psal' THEN m.value END) AS salinity,
    MIN(m.juld) AS juld
  FROM measurements m
  WHERE m.float_id = :p5 AND m.cycle = :p6
  GROUP BY m.depth_m
)
SELECT
  :p5 AS float_id, :p6 AS cycle, NULL::INT AS profile_number,
  f.latitude AS lat, f.longitude AS lon, pivoted.juld,
  pivoted.depth_m, pivoted.temperature, pivoted.salinity
FROM (SELECT DISTINCT float_id, cycle FROM measurements WHERE float_id = :p5 AND cycle = :p6 LIMIT 1) d
JOIN floats f ON f.float_id = d.float_id AND f.cycle = d.cycle
JOIN pivoted ON TRUE
ORDER BY pivoted.depth_m
LIMIT :p0"""
  },
  # TECH parameters (with context via floats)
{
  "title": "TECH PARAMETERS FOR FLOAT (STRICT)",
  "sql": """\
SELECT
  t.float_id,
  t.cycle,
  f.profile_number,
  f.latitude AS lat,
  f.longitude AS lon,
  f.juld,
  t.param_name,
  t.param_value,
  t.units,
  t.collected_at,
  t.inserted_at
FROM tech t
LEFT JOIN floats f
  ON f.float_id = t.float_id
 AND f.cycle    = t.cycle
WHERE t.float_id = :p1
ORDER BY t.cycle DESC, f.juld DESC NULLS LAST, t.param_name
LIMIT :p0"""
},

]
