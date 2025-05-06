CREATE TABLE IF NOT EXISTS f_climate_metric (
	metric_id BIGINT NOT NULL,
	gas_type VARCHAR(255), 
	emissions_qty FLOAT,
	emissions_factor_ton FLOAT,
	capacity_ton FLOAT,
	capacity_factor FLOAT, 
	activity_ton FLOAT,
	model_methodology VARCHAR(255),
	creation_date DATE,
	geo_id BIGINT,
	sector_id BIGINT,
	time_id BIGINT,
	source_id BIGINT,
	factor_id BIGINT,

	CONSTRAINT pk_f_climate_metric PRIMARY KEY(metric_id)
);

CREATE INDEX idx_gas_type ON f_climate_metric(gas_type);
CREATE INDEX idx_model_methodology ON f_climate_metric(model_methodology);

COMMENT ON TABLE f_climate_metric IS 'Central fact table for climate-related metrics, recording emissions, capacity, activity levels, and associated factors.';
COMMENT ON COLUMN f_climate_metric.metric_id IS 'Unique identifier for each record in the fact table.';
COMMENT ON COLUMN f_climate_metric.gas_type IS 'Type of greenhouse gas (e.g., CO2, CH4).';
COMMENT ON COLUMN f_climate_metric.emissions_qty IS 'Total emissions quantity in tons.';
COMMENT ON COLUMN f_climate_metric.emissions_factor_ton IS 'Emissions factor expressed as tons of gas per ton of aluminum.';
COMMENT ON COLUMN f_climate_metric.capacity_ton IS 'Total production or operational capacity measured in tons per ton of aluminum.';
COMMENT ON COLUMN f_climate_metric.capacity_factor IS 'Ratio indicating operational efficiency or utilization.';
COMMENT ON COLUMN f_climate_metric.activity_ton IS 'Activity levels, such as production or operations, measured in tons per ton of aluminum.';
COMMENT ON COLUMN f_climate_metric.model_methodology IS 'Description of the methodology used for modeling emissions or activity.';
COMMENT ON COLUMN f_climate_metric.creation_date IS 'Date the record was created.';
COMMENT ON COLUMN f_climate_metric.geo_id IS 'Foreign key referencing the geographical dimension (d_geography).';
COMMENT ON COLUMN f_climate_metric.sector_id IS 'Foreign key referencing the sector dimension (d_sector).';
COMMENT ON COLUMN f_climate_metric.time_id IS 'Foreign key referencing the time dimension (d_time).';
COMMENT ON COLUMN f_climate_metric.source_id IS 'Foreign key referencing the emission source dimension (d_emission_source).';
COMMENT ON COLUMN f_climate_metric.factor_id IS 'Foreign key referencing the emission factor dimension (d_emission_factor).';
COMMENT ON INDEX idx_gas_type IS 'Index for efficient lookups using the gas_type column.';
COMMENT ON INDEX idx_model_methodology IS 'Index for efficient filtering and searching based on model methodology.';

-- d_geography
CREATE TABLE IF NOT EXISTS d_geography(
	geo_id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	gadm VARCHAR(255),
	country_code CHAR(3),
	geometry_ref VARCHAR(255),
	latitude FLOAT,
	longitude FLOAT,

	CONSTRAINT pk_d_geography PRIMARY KEY(geo_id)
);

CREATE INDEX idx_geometry_ref ON d_geography(geometry_ref);
CREATE INDEX idx_country_code ON d_geography(country_code);

COMMENT ON TABLE d_geography IS 'Dimension table providing geographical information, including spatial references, location coordinates, and country codes.';
COMMENT ON COLUMN d_geography.geo_id IS 'Unique identifier for each geographic entry.';
COMMENT ON COLUMN d_geography.gadm IS 'GADM administrative level identifier for geographic boundaries.';
COMMENT ON COLUMN d_geography.country_code IS 'ISO 3166-1 alpha-3 code for countries.';
COMMENT ON COLUMN d_geography.geometry_ref IS 'Reference value linking to external geometry definitions.';
COMMENT ON COLUMN d_geography.latitude IS 'Latitude coordinate of the location.';
COMMENT ON COLUMN d_geography.longitude IS 'Longitude coordinate of the location.';
COMMENT ON INDEX idx_geometry_ref IS 'Index for efficient lookups using the geometry reference column.';
COMMENT ON INDEX idx_country_code IS 'Index for efficient filtering and searching based on country codes.';


-- d_sector
CREATE TABLE IF NOT EXISTS d_sector(
	sector_id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	sector VARCHAR(255),
	subsector VARCHAR(255),
	original_inventory_sector VARCHAR(255),

	CONSTRAINT pk_d_sector PRIMARY KEY(sector_id),
	CONSTRAINT uq_d_sector UNIQUE(sector, subsector)
);

COMMENT ON TABLE d_sector IS 'Dimension table providing information about sectors and their corresponding subsectors, including original inventory sector classifications.';
COMMENT ON COLUMN d_sector.sector_id IS 'Unique identifier for each sector entry in the table. Acts as the primary key.';
COMMENT ON COLUMN d_sector.sector IS 'High-level description or name of the sector (e.g., Energy, Agriculture).';
COMMENT ON COLUMN d_sector.subsector IS 'Subcategory within the sector providing additional granularity (e.g., Power Generation under Energy).';
COMMENT ON COLUMN d_sector.original_inventory_sector IS 'Original classification of the sector from the source inventory or dataset.';

-- d_time
CREATE TYPE month_enum AS ENUM ('JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC');
CREATE TYPE granularity_enum AS ENUM ('year', 'month', 'day', 'hour', 'minute');
CREATE TABLE IF NOT EXISTS d_time (
	time_id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	year INT,
	month month_enum,
	quarter CHAR(2),
	start_date DATE,
	end_date DATE,
	temporal_granularity granularity_enum,

	CONSTRAINT pk_d_time PRIMARY KEY(time_id)
);

COMMENT ON TYPE month_enum IS 'Enumeration type for months of the year, represented by their three-letter abbreviations (e.g., JAN, FEB, etc.).';
COMMENT ON TYPE granularity_enum IS 'Enumeration type representing temporal granularity, such as year, month, day, hour, or minute, used to specify the level of detail for temporal data.';
COMMENT ON TABLE d_time IS 'Dimension table for time-related attributes, used to organize and analyze data by different time granularities, including years, months, quarters, and date ranges.';
COMMENT ON COLUMN d_time.time_id IS 'Unique identifier for each time entry in the table. Serves as the primary key.';
COMMENT ON COLUMN d_time.year IS 'Calendar year associated with the time entry (e.g., 2025).';
COMMENT ON COLUMN d_time.month IS 'Month of the year, stored as an ENUM type (e.g., JAN, FEB, MAR).';
COMMENT ON COLUMN d_time.quarter IS 'Quarter of the year, represented as a two-character string (e.g., Q1, Q2).';
COMMENT ON COLUMN d_time.start_date IS 'Start date for the time entry, defining the beginning of the time period.';
COMMENT ON COLUMN d_time.end_date IS 'End date for the time entry, defining the end of the time period.';
COMMENT ON COLUMN d_time.temporal_granularity IS 'Granularity of the time period, represented as an ENUM type (e.g., year, month, day). Used to specify the level of detail for the time entry.';


-- d_emission_source
CREATE TABLE IF NOT EXISTS d_emission_source (
	source_id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	source_name TEXT,
	source_type TEXT,

	CONSTRAINT pk_d_emission_source PRIMARY KEY(source_id),
	CONSTRAINT uq_source_name UNIQUE(source_name)
);

COMMENT ON TABLE d_emission_source IS 'Dimension table for emission sources, identifying and categorizing various origins of emissions data. Ensures uniqueness of each source by name and type.';
COMMENT ON COLUMN d_emission_source.source_id IS 'Unique identifier for each emission source. Automatically generated as an identity column and serves as the primary key.';
COMMENT ON COLUMN d_emission_source.source_name IS 'Name of the emission source (e.g., power plant, industrial site).';
COMMENT ON COLUMN d_emission_source.source_type IS 'Type or category of the emission source (e.g., transportation, agriculture, manufacturing).';
-- COMMENT ON CONSTRAINT uq_source_name_type IS 'Ensures that each combination of source name and source type is unique, preventing duplicate entries.';

-- d_emission_factor
CREATE TABLE IF NOT EXISTS d_emission_factor(
	factor_id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	total_emissions_factor_ton FLOAT,
	total_emissions_ton FLOAT,
	electricity_use_factor_MWh FLOAT,
	electricity_use_MWh FLOAT,
	grid_emissions_intensity_ton FLOAT,

	CONSTRAINT pk_d_emission_factor PRIMARY KEY(factor_id)
);

COMMENT ON TABLE d_emission_factor IS 'Dimension table for emission factors, recording metrics related to emissions, electricity use, and grid intensity to support analysis and calculations.';
COMMENT ON COLUMN d_emission_factor.factor_id IS 'Unique identifier for each emission factor entry. Automatically generated as an identity column and serves as the primary key.';
COMMENT ON COLUMN d_emission_factor.total_emissions_factor_ton IS 'Total emissions factor measured in tons, representing emissions per unit of activity or production.';
COMMENT ON COLUMN d_emission_factor.total_emissions_ton IS 'Total emissions measured in tons.';
COMMENT ON COLUMN d_emission_factor.electricity_use_factor_MWh IS 'Electricity usage factor measured in megawatt-hours (MWh), representing electricity usage per unit of activity or production.';
COMMENT ON COLUMN d_emission_factor.electricity_use_MWh IS 'Total electricity usage measured in megawatt-hours (MWh).';
COMMENT ON COLUMN d_emission_factor.grid_emissions_intensity_ton IS 'Grid emissions intensity measured in tons, representing the emissions produced per unit of electricity consumed.';
