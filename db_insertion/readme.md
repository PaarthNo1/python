-- Run once per database (requires superuser)
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pg_trgm; -- optional: fast text search
CREATE EXTENSION IF NOT EXISTS vector; -- pgvector extension; name may be `vector` or `pgvector` depending on install

---

TRUNCATE TABLE floats RESTART IDENTITY;
TRUNCATE TABLE measurements RESTART IDENTITY;
TRUNCATE TABLE profiles RESTART IDENTITY;
TRUNCATE TABLE meta_kv RESTART IDENTITY;
TRUNCATE TABLE traj RESTART IDENTITY;
TRUNCATE TABLE tech RESTART IDENTITY;
TRUNCATE TABLE sensors_catalog RESTART IDENTITY;

SELECT COUNT(_) FROM floats;
SELECT COUNT(_) FROM measurements;
SELECT COUNT(_) FROM profiles;
SELECT COUNT(_) FROM meta_kv;
SELECT COUNT(_) FROM traj;
SELECT COUNT(_) FROM tech;
SELECT COUNT(\*) FROM sensors_catalog;

SELECT _ FROM floats LIMIT 10;
SELECT _ FROM measurements LIMIT 10;
SELECT _ FROM profiles LIMIT 10;
SELECT _ FROM meta_kv LIMIT 10;
SELECT _ FROM traj LIMIT 10;
SELECT _ FROM tech LIMIT 10;
SELECT \* FROM sensors_catalog LIMIT 10;

---

CREATE TABLE floats (
float_id VARCHAR(32) NOT NULL,
cycle INTEGER NOT NULL,
profile_number INTEGER,
wmo_id VARCHAR(32),
platform_type VARCHAR(64),
project_name TEXT,
pi_name TEXT,
latitude DOUBLE PRECISION,
longitude DOUBLE PRECISION,
juld TIMESTAMP WITH TIME ZONE,
source_file TEXT, -- filename source
end_mission_status TEXT, -- [NEW] Mission status
end_mission_date TIMESTAMP WITH TIME ZONE, -- [NEW] Mission end date
created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
PRIMARY KEY (float_id, cycle)
);

-- Spatial convenience column (PostGIS)
ALTER TABLE floats ADD COLUMN geom GEOMETRY(Point, 4326);
UPDATE floats SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
WHERE latitude IS NOT NULL AND longitude IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_floats_geom ON floats USING GIST (geom);

-- Useful indexes
CREATE INDEX IF NOT EXISTS idx_floats_juld ON floats (juld);
CREATE INDEX IF NOT EXISTS idx_floats_wmo ON floats (wmo_id);

-- Parent partitioned table (partition by RANGE on juld for time-based partitioning)
CREATE TABLE measurements (
id BIGSERIAL,
float_id VARCHAR(32) NOT NULL,
cycle INTEGER NOT NULL,
profile_number INTEGER,
juld TIMESTAMP WITH TIME ZONE NOT NULL, -- REQUIRED
latitude DOUBLE PRECISION,
longitude DOUBLE PRECISION,

    depth_m         DOUBLE PRECISION NOT NULL,
    sensor          VARCHAR(64) NOT NULL,
    value           DOUBLE PRECISION,
    qc              TEXT,
    source_file     TEXT,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    PRIMARY KEY (id, juld)

)
PARTITION BY RANGE (juld);

DO $$
DECLARE y int;
BEGIN
    FOR y IN 2000..2030 LOOP
        EXECUTE format(
            'CREATE TABLE measurements_%s
             PARTITION OF measurements
             FOR VALUES FROM (%L) TO (%L);',
             y, y || '-01-01', (y+1) || '-01-01'
        );
    END LOOP;
END$$;

ALTER TABLE measurements
ADD CONSTRAINT uniq_measurement_all
UNIQUE (float_id, cycle, profile_number, depth_m, sensor, source_file, juld);

-- Indexes for fast queries
CREATE INDEX IF NOT EXISTS idx_meas_float ON measurements (float_id, cycle, profile_number);
CREATE INDEX IF NOT EXISTS idx_meas_sensor ON measurements (sensor);
CREATE INDEX IF NOT EXISTS idx_meas_juld ON measurements (juld);
CREATE INDEX IF NOT EXISTS idx_meas_geom ON measurements USING BRIN (latitude, longitude); -- BRIN for large datasets

-- Profiles table
CREATE TABLE profiles (
float_id VARCHAR(32) NOT NULL,
cycle INTEGER NOT NULL,
profile_number INTEGER,
juld TIMESTAMP WITH TIME ZONE,
lat DOUBLE PRECISION,
lon DOUBLE PRECISION,
pres DOUBLE PRECISION[], -- array of pressures
temp DOUBLE PRECISION[], -- array of temps, align with pres
psal DOUBLE PRECISION[],
temp_qc TEXT[],
psal_qc TEXT[],
source_file TEXT,
geom GEOMETRY(Point, 4326),
created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
PRIMARY KEY (float_id, cycle)
);

CREATE INDEX IF NOT EXISTS idx_profiles_geom ON profiles USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_profiles_juld ON profiles (juld);

-- meta kv
CREATE TABLE meta_kv (
id BIGSERIAL PRIMARY KEY,
float_id VARCHAR(32),
var_name TEXT NOT NULL, -- e.g., PLATFORM_NUMBER or "title"
attr_name TEXT, -- NULL for variable itself, otherwise attribute key like "long_name"
value_text TEXT, -- text representation of value(s)
dtype TEXT,
shape TEXT,
source_file TEXT,
inserted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_meta_float ON meta_kv (float_id);
CREATE INDEX IF NOT EXISTS idx_meta_var ON meta_kv USING GIN (var_name gin_trgm_ops);

-- traj
CREATE TABLE traj (
id BIGSERIAL PRIMARY KEY,
float_id VARCHAR(32),
cycle INTEGER,
profile_number INTEGER,
juld TIMESTAMP WITH TIME ZONE,
latitude DOUBLE PRECISION,
longitude DOUBLE PRECISION,
position_qc TEXT,
location_system TEXT,
juld_qc TEXT, -- [NEW]
positioning_system TEXT, -- [NEW]
source_file TEXT,
geom GEOMETRY(Point, 4326),
created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_traj_float ON traj (float_id, cycle);
CREATE INDEX idx_traj_geom ON traj USING GIST (geom);
CREATE INDEX idx_traj_juld ON traj (juld);

--tech
CREATE TABLE tech (
id BIGSERIAL PRIMARY KEY,
float_id VARCHAR(32),
cycle INTEGER,
param_name TEXT,
param_value TEXT,
units TEXT,
source_file TEXT,
collected_at TIMESTAMP WITH TIME ZONE,
inserted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_tech_float ON tech (float_id, cycle);
CREATE INDEX idx_tech_param ON tech (param_name);

--sensor catalog
CREATE TABLE sensors_catalog (
sensor_id SERIAL PRIMARY KEY,
sensor_name VARCHAR(64) UNIQUE,
model TEXT,
manufacturer TEXT,
units TEXT,
description TEXT,
calibration_meta JSONB,
created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- [NEW] Summary Materialized View
CREATE MATERIALIZED VIEW IF NOT EXISTS float_summary_mv AS
SELECT
f.float_id,
MAX(p.cycle) as last_cycle,
MAX(p.juld) as last_profile_date,
COUNT(p.profile_number) as num_profiles,
(ARRAY_AGG(p.lat ORDER BY p.juld DESC))[1] as last_lat,
(ARRAY_AGG(p.lon ORDER BY p.juld DESC))[1] as last_lon,
CASE
WHEN MAX(p.juld) > NOW() - INTERVAL '30 days' THEN 'Active'
ELSE 'Inactive'
END as status
FROM floats f
LEFT JOIN profiles p ON f.float_id = p.float_id
GROUP BY f.float_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_float_summary_mv_fid ON float_summary_mv (float_id);
