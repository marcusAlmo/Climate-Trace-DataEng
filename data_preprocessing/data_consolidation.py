import duckdb

# Define your parquet file paths
parquet_files = [
    f"../cleaned_data/{year}/cleaned_{name}.parquet" for name, year in {
        "2021_ch4.parquet": 2021, "2021_co2.parquet": 2021, "2021_n2o.parquet": 2021,
        "2022_ch4.parquet": 2022, "2022_co2.parquet": 2022, "2022_n2o.parquet": 2022,
        "2023_ch4.parquet": 2023, "2023_co2.parquet": 2023, "2023_n2o.parquet": 2023,
        "2024_ch4.parquet": 2024, "2024_co2.parquet": 2024, "2024_n2o.parquet": 2024,
    }.items()
]

output_path = "../cleaned_data/merged_climate_data.parquet"

# Use DuckDB to merge
duckdb.query(f"CREATE TABLE merged AS SELECT * FROM read_parquet({parquet_files})")
duckdb.query(f"COPY merged TO '{output_path}' (FORMAT 'parquet')")
print(f"Successfully merged into {output_path}")