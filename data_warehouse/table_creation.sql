-- SQLite Dialect for Climate Trace Data Warehouse

-- f_climate_metrics
CREATE TABLE IF NOT EXISTS f_climate_metrics (
	metric_id INTEGER PRIMARY KEY,
	gas_type TEXT, 
	emissions_qty REAL,
	emissions_factor_ton REAL,
	capacity_ton REAL,
	capacity_factor REAL, 
	activity_ton REAL,
	model_methodology TEXT,
	creation_date TEXT,
	geo_id INTEGER,
	sector_id INTEGER,
	time_id INTEGER,
	source_id INTEGER,
	factor_id INTEGER
);

CREATE INDEX IF NOT EXISTS idx_gas_type ON f_climate_metrics(gas_type);
CREATE INDEX IF NOT EXISTS idx_model_methodology ON f_climate_metrics(model_methodology);

-- d_geography
CREATE TABLE IF NOT EXISTS d_geography(
	geo_id INTEGER PRIMARY KEY AUTOINCREMENT,
	gadm TEXT,
	country_code TEXT,
	geometry_ref TEXT,
	latitude REAL,
	longitude REAL
);

CREATE INDEX IF NOT EXISTS idx_geometry_ref ON d_geography(geometry_ref);
CREATE INDEX IF NOT EXISTS idx_country_code ON d_geography(country_code);

-- d_sector
CREATE TABLE IF NOT EXISTS d_sector(
	sector_id INTEGER PRIMARY KEY AUTOINCREMENT,
	sector TEXT,
	subsector TEXT,
	original_inventory_sector TEXT,
	UNIQUE(sector, subsector)
);

-- d_time
CREATE TABLE IF NOT EXISTS d_time (
	time_id INTEGER PRIMARY KEY AUTOINCREMENT,
	year INTEGER,
	month TEXT,
	quarter TEXT,
	start_date TEXT,
	end_date TEXT,
	temporal_granularity TEXT
);

-- d_emission_source
CREATE TABLE IF NOT EXISTS d_emission_source (
	source_id INTEGER PRIMARY KEY AUTOINCREMENT,
	source_name TEXT UNIQUE,
	source_type TEXT
);

-- d_emission_factor
CREATE TABLE IF NOT EXISTS d_emission_factor(
	factor_id INTEGER PRIMARY KEY AUTOINCREMENT,
	total_emissions_factor_ton REAL,
	total_emissions_ton REAL,
	electricity_use_factor_MWh REAL,
	electricity_use_MWh REAL,
	grid_emissions_intensity_ton REAL
);

-- Staging table
CREATE TABLE IF NOT EXISTS staging_climate_trace (
    source_id INTEGER, 
    iso3_country TEXT,
    sector TEXT,
    subsector TEXT,
    original_inventory_sector TEXT,
    start_time TEXT,
    end_time TEXT,
    temporal_granularity TEXT,
    gas TEXT,
    emissions_quantity REAL,
    emissions_factor REAL,
    emissions_factor_units TEXT,
    capacity REAL,
    capacity_units TEXT,
    capacity_factor REAL,
    activity REAL,
    activity_units TEXT,
    created_date TEXT,
    modified_date TEXT,
    source_name TEXT,
    source_type TEXT,
    lat REAL,
    lon REAL,
    other1 TEXT,
    other2 TEXT,
    other3 TEXT,
    other4 TEXT,
    other5 TEXT,
    other6 TEXT,
    other1_def TEXT,
    other2_def TEXT,
    other3_def TEXT,
    other4_def TEXT,
    other5_def TEXT,
    other6_def TEXT,
    geometry_ref TEXT,
    conf_source_type TEXT,
    conf_capacity TEXT,
    conf_capacity_factor TEXT,
    conf_activity TEXT,
    conf_emissions_factor TEXT,
    conf_emissions_quantity TEXT,
    year INTEGER
);