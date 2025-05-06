# Project Directory Structure
root
├── cleaned_data
├── data_preprocessing
├── data_warehouse
├── raw_data


## Tech Stacks and Dependencies

1. **Programming Language**
   - Python

2. **Data Format**
   - Parquet

3. **Python Data Frameworks**
   - Dask
   - Pandas
   - Numpy

## Python Script to Generate the Structure

```python
import os

# Define root directory and subdirectories
root_dir = "root"
sub_dirs = ["cleaned_data", "data_preprocessing", "data_warehouse", "raw_data"]

# Years range
years = range(2021, 2025)

# Parquet file templates
parquet_files = [
    "{year}_ch4.parquet",
    "{year}_co2.parquet",
    "{year}_n2o.parquet",
    "{year}_co2e_100yr.parquet"
]

# Create root and subdirectories
for sub_dir in sub_dirs:
    sub_dir_path = os.path.join(root_dir, sub_dir)
    os.makedirs(sub_dir_path, exist_ok=True)

    # Create year subdirectories and files only for cleaned_data and raw_data
    if sub_dir in ["cleaned_data", "raw_data"]:
        for year in years:
            year_dir = os.path.join(sub_dir_path, str(year))
            os.makedirs(year_dir, exist_ok=True)

            for file_template in parquet_files:
                file_path = os.path.join(year_dir, file_template.format(year=year))
                open(file_path, 'w').close()  # Create empty file

print("Project directory structure successfully created.")
