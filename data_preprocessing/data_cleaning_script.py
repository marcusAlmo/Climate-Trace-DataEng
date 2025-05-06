# %% [markdown]
# ## DATA CLEANING

# %%
# !pip install tabulate

# %%
import dask.dataframe as dd

# %%
"""
 Cleaned Data Parquet Files:
 -2021-
 [1] 2021_ch4.parquet
 [2] 2021_co2.parquet
 [3] 2021_n2o.parquet
 [4] 2021_co2e_100yr.parquet

 -2022-
 [5] 2022_ch4.parquet
 [6] 2022_co2.parquet
 [7] 2022_n2o.parquet

"""

# %%
# Parquet Files
parquet_files = {
    "2021_ch4.parquet" : 2021,
    "2021_co2.parquet" : 2021,
    "2021_n2o.parquet" : 2021,
    "2021_co2e_100yr.parquet" : 2021,
    "2022_ch4.parquet" : 2022,
    "2022_co2.parquet" : 2022,
    "2022_n2o.parquet" : 2022,
    "2022_co2e_100yr.parquet" : 2022,
    "2023_ch4.parquet" : 2023,
    "2023_co2.parquet" : 2023,
    "2023_n2o.parquet" : 2023,
    "2023_co2e_100yr.parquet" : 2023,
    "2024_ch4.parquet" : 2024,
    "2024_co2.parquet" : 2024,
    "2024_n2o.parquet" : 2024,
    "2024_co2e_100yr.parquet" : 2024
}
print("Parquet Files in the directory:\n")
for each in parquet_files:
    print(each)

target_parquet = input("Enter the target parquet file name to upload: ")
df = None

if target_parquet in parquet_files:
    parquet_file = f"../raw_data/{parquet_files[target_parquet]}/{target_parquet}"
    df = dd.read_parquet(parquet_file, npartitions=10)
    print("Parquet file loaded successfully.")
else:
    print("Invalid input. Please enter a valid parquet file name.")

# %%
# Drop Noise Columns
df = df.drop(columns=["other7", "other8", "other9", "other10", "other11", "other12",
                      "other1_def", "other2_def", "other3_def", "other4_def", "other5_def", "other6_def",
                      "other7_def", "other8_def", "other9_def", "other10_def", "other11_def", "other12_def",
                      "conf_source_type", "conf_capacity", "conf_capacity_factor", "conf_activity", "conf_emissions_factor", "conf_emissions_quantity",
                      "emissions_factor_units", "capacity_units", "activity_units"])
df = df.persist()
print("Successfully dropped noise columns. Their data can be found in the documentation.")

# %%
# Memory Usage Check
memory_usage = df.memory_usage(deep=True).compute()
memory_in_mb = memory_usage / (1024 * 1024)

print("Memory Usage (MB):")
for column, mb in memory_in_mb.items():
    print(f"{column}: {mb:.2f} MB")

print(f"\nTotal Memory: {memory_in_mb.sum():.2f} MB")

# %%
print(df.head())
print(df.dtypes)

# %%
# Rename lat and lon to latitude and longitude
df = df.rename(columns={'lat': 'latitude', 'lon': 'longitude'})
df = df.persist()

print("Successfully renamed lat and lon to latitude and longitude")

# %%
"""
 iso3_country review
"""
# Assuming df is already a Dask DataFrame
iso3_countries = df['iso3_country'].unique().compute()
print("Unique Countries:")

for i, country in enumerate(iso3_countries, 1):
    print(f"{country}", end="\t")
    if i % 5 == 0:
        print() 

if len(iso3_countries) % 5 != 0:
    print()

"""
    There are 2 countries that are not in the ISO 3166-1 alpha-3 standard:
    - UNK
    - ZNC

    UNK is unknown and ZNC is a country code in Africa for unlisted countries.
    Replace ZNC with UNK.
"""

# Replace ZNC with UNK
df = df.assign(iso3_country=df['iso3_country'].replace({'ZNC': 'UNK'}))
df = df.persist()

# Compute to verify no ZNC remains
assert 'ZNC' not in df['iso3_country'].unique().compute()
print("Successfully replaced ZNC with UNK.")

# %%
# !pip install pycountry

# %%
import pycountry

def get_country_name(iso3_code):
    """
    Convert ISO 3-letter country code to full country name
    Handle special cases like 'UNK' and 'ZNC'
    """
    special_cases = {
        'UNK': 'Unknown',
        'ZNC': 'Unlisted African Countries'
    }
    
    if iso3_code in special_cases:
        return special_cases[iso3_code]
    
    try:
        return pycountry.countries.get(alpha_3=iso3_code).name
    except AttributeError:
        return f"Unknown Country: {iso3_code}"

def country_name_mapper(iso3_code):
    return get_country_name(iso3_code)

df = df.assign(
    country_code=df['iso3_country'],
    country_name=df['iso3_country'].map(country_name_mapper, meta=('country_name', 'object'))
)
df = df.drop(columns=['iso3_country'])

df = df.persist()

print(df.head())

# %%
# Country Code Verification
from tabulate import tabulate

# Count occurrences of each country code
country_counts = df['country_code'].value_counts().compute()

# Convert to a list of tuples for tabulation
country_table = [(country, count) for country, count in country_counts.items()]

# Sort the table by count in descending order
country_table.sort(key=lambda x: x[1], reverse=True)

# Print the table with headers
print(tabulate(
    country_table, 
    headers=['Country Code', 'Count'], 
    tablefmt='grid'
))

# Print total number of unique countries
print(f"\nTotal Unique Countries: {len(country_counts)}")

# Percentage distribution
total_records = country_counts.sum()
country_percentages = [(country, count, (count/total_records)*100) for country, count in country_counts.items()]
country_percentages.sort(key=lambda x: x[1], reverse=True)

print("\nCountry Distribution:")
print(tabulate(
    [(country, count, f"{percentage:.2f}%") for country, count, percentage in country_percentages], 
    headers=['Country Code', 'Count', 'Percentage'], 
    tablefmt='grid'
))

# %%
# Sector, Subsector, Original Inventory Sector Review
unique_sector = df['sector'].unique().compute()
print("\nUnique Sectors:")
for i in unique_sector:
    print(i)

unique_subsector = df['subsector'].unique().compute()
print("\nUnique Subsectors:")
for i in unique_subsector:
    print(i)

unique_original_sector = df['original_inventory_sector'].unique().compute()
print("\nUnique Original Sectors:")
for i in unique_original_sector:
    print(i)

"""
    the sector and subsector fields are ready to be used but the original_inventory_sector field is empty
    and should be dropped
"""


# %%
# Drop original_inventory_sector column
df = df.drop(columns=['original_inventory_sector']).persist()

print("Successfully dropped original_inventory_sector column")
df.dtypes

# %%
# Start Time and End Time Review

unique_start_time = df['start_time'].unique().compute()
unique_end_time = df['end_time'].unique().compute()

unique_start_time = unique_start_time.sort_values()
unique_end_time = unique_end_time.sort_values()

print("Unique Start Times")
for i in unique_start_time:
    print(i)
print("\n\nUnique End Times")
for i in unique_end_time:
    print(i)

"""
    these timestamps are saved in string format
    we need to convert them to date format, removing the timezone and time
"""

# %%

# Dask start and end time dtype transformation
df['start_time'] = dd.to_datetime(df['start_time'])
df['end_time'] = dd.to_datetime(df['end_time'])

df = df.rename(columns={'start_time': 'start_date', 'end_time': 'end_date'})

# Persist the DataFrame
df = df.persist()

# Check the dtypes
print(df['start_date'].dtype)
print(df['end_date'].dtype)


# %%
# Temporal Granularity Review
unique_granularity = df['temporal_granularity'].unique().compute()
print(unique_granularity)

"""
    month is the temporal granularity of the data
    therefore, we will drop the temporal granularity column
"""

# %%
# Drop temporal granularity
df = df.drop(columns=['temporal_granularity'])
df = df.persist()

print("Successfully dropped temporal granularity")
df.head()

# %%
# Gas Review
unique_gas = df['gas'].unique().compute()
print(unique_gas)


# %%
# Emissions Quantity Review
print(df['emissions_quantity'].describe().compute())

"""
    the emissions quantity is in metric tonnes and is consistently a float value
"""

# %%
# Emissions Quantity Unit
df = df.rename(columns={'emissions_quantity': 'emissions_quantity_ton'})
df = df.persist()

df['emissions_quantity_ton'].describe().compute()

# %%
# Emissions Factor
print(df['emissions_factor'].describe().compute())

"""
    emissions_factor is in metric tonnes of CO2 per t of aluminum
"""

# %%
# Emissions Factor Rename
df = df.rename(columns={'emissions_factor': 'emissions_factor_ton'})
df = df.persist()

print("Successfully renamed emissions_factor to emissions_factor_ton")

# %%
# Capacity Review
print(df['capacity'].describe().compute())

"""
    the capacity column has a mean of inf and std of inf
    this is because of the presence of inf values
"""

# %%
# Fix the capacity column
# Replace inf with nan
import numpy as np

df['capacity'] = df['capacity'].replace([np.inf, -np.inf], np.nan)
df = df.persist()

df['capacity'].describe().compute()

# %%
# Capacity Rename
# Rename capacity column to capacity_ha
# Drop capacity_units column

df = df.rename(columns={'capacity': 'capacity_ha'})
df = df.persist()

print("Successfully renamed capacity column to capacity_ha")

# %%
# Capacity Factor Review
print(df['capacity_factor'].describe().compute())

"""
    they are consistently float values and are ready for analysis
"""

# %%
# Activity Review
print(df['activity'].describe().compute())

"""
    they are consistently float values and are ready for analysis

    this is in hectare of land area
"""

# %%
# Activity Rename
df = df.rename(columns={'activity': 'activity_ha'})
df = df.persist()

print("Successfully renamed activity column to activity_ha")

# %%
# Drop created_date and modified_date column
df = df.drop(columns=['created_date', 'modified_date'])
df = df.persist()

print("Successfully dropped created_date and modified_date column")


# %%
# Source Name Review
unique_source_names = df['source_name'].unique().compute()
for name in unique_source_names:
    print(name)

"""
   source name is unnecessary and can be dropped 
"""

# %%
# Drop Source Name
df = df.drop(columns=['source_name'])
df = df.persist()

print("Successfully dropped source_name column")

# %%
# Source Type Review
unique_source_types = df['source_type'].unique().compute()
for source_type in unique_source_types:
    print(source_type)

"""
   source type is unnecessary and can be dropped 
"""

# %%
# Drop Source Type
df = df.drop(columns=['source_type'])
df = df.persist()

print("Successfully dropped source_type column")


# %%
# Other Column Review

unique_values = df['other1'].unique().compute()
print(unique_values)

unique_values = df['other2'].unique().compute()
print(unique_values)

unique_values = df['other3'].unique().compute()
print(unique_values)

unique_values = df['other4'].unique().compute()
print(unique_values)

unique_values = df['other5'].unique().compute()
print(unique_values)

unique_values = df['other6'].unique().compute()
print(unique_values)

"""
There are inconsistencies in the data stored in the other columns
which can be considered noise. 

Dropping the six supplementary columns will be done.

"""


# %%
# Drop other columns
df = df.drop(columns=['other1', 'other2', 'other3', 'other4', 'other5', 'other6'])

df = df.persist()
print("Successfully dropped other columns")

# %%
# Review Geometry Ref
print(df['geometry_ref'].unique().compute())

"""
    geometry_ref have inconsistencies
    some records have "trace_" prefix
    some records have "gadm_" prefix

    those records with "gadm_" prefix are difficult to join with the climate trace location points
    dropping this column will not affect the analysis as the longitude and latitude columns are still available
"""

# %%
# Drop Geometry Ref
df = df.drop(columns=["geometry_ref"])
df = df.persist()
print("Successfully dropped geometry_ref column.")



# %%
# Add month and quarter columns using start_date column
df['month'] = df['start_date'].dt.month
df['quarter'] = df['start_date'].dt.quarter

df = df.persist()
print(df['month'].compute().unique())
print(df['quarter'].compute().unique())

print("Successfully added month and quarter columns")

# %%
# Final Dataset Check
print(f"""Count:\n 
{df.count()}
==============================================
""")
print(f"""Head:\n 
{df.head()}
==============================================
""")
print(f"""Tail:\n 
{df.tail()}
==============================================
""")

print(  f"""Describe:\n 
{df.describe()}
==============================================
""")
print(f"""Dtypes:\n 
{df.dtypes}
==============================================
""")

# %%
# Export Preparations
# Convert float columns that should be integers
df['source_id'] = df['source_id'].astype('int64')
df['year'] = df['year'].astype('int64')

# Convert object columns to more efficient types
df['country_name'] = df['country_name'].astype('category')

# rearrange columns
final_columns = [
    'source_id', 
    'gas',
    'emissions_quantity_ton',
    'emissions_factor_ton',
    'capacity_ha',
    'capacity_factor',
    'activity_ha',
    'sector',
    'subsector',
    'country_name',
    'country_code',
    'latitude',
    'longitude',
    'year',
    'quarter',
    'month',
    'start_date',
    'end_date',
    ]
    
df = df[final_columns]
df = df.compute()
print('Preparations done successfully. Proceed to export.')


# %%
# Dataset Export
try:
    df.to_parquet(
        f'../cleaned_data/{parquet_files[target_parquet]}/cleaned_{target_parquet}.parquet', 
        engine='pyarrow', 
        compression='snappy'
    )

    print("Successful data export")

    del df
    import gc
    gc.collect()

except PermissionError:
    print("Permission denied. Check:")
    print("1. Close any applications using the file")
    print("2. Check write permissions")
    print("3. Ensure file is not read-only")



