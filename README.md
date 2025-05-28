# 🌍 Climate Trace Data Engineering Project

![GitHub last commit](https://img.shields.io/github/last-commit/yourusername/Climate-Trace-DataEng)
![GitHub repo size](https://img.shields.io/github/repo-size/yourusername/Climate-Trace-DataEng)
![Python](https://img.shields.io/badge/python-3.8%2B-blue)
![License](https://img.shields.io/badge/License-MIT-green)

## 📋 Overview
This project is a comprehensive data engineering pipeline for processing and analyzing climate trace data. It's designed to handle large-scale environmental datasets from Climate Trace, focusing on greenhouse gas emissions (CO₂, CH₄, N₂O) and their CO₂ equivalents.

## 🏗️ Project Structure

```
Climate-Trace-DataEng/
├── data_preprocessing/       # Data cleaning and transformation
│   ├── spark/               # Spark jobs for big data processing
│   │   ├── Data Exploration
│   │   ├── Data Wrangling
│   │   └── Data Consolidation
│   └── README.md           # Preprocessing documentation
├── data_warehouse/         # Data warehouse implementation
│   ├── climate_trace_erd.vsdx
│   ├── staging_table_creation.sql
│   └── table_creation.sql
├── raw_data/               # Raw data storage (gitignored)
│   └── {year}/
│       ├── {year}_ch4.parquet
│       ├── {year}_co2.parquet
│       ├── {year}_n2o.parquet
│       └── {year}_co2e_100yr.parquet
└── cleaned_data/           # Processed data (gitignored)
    └── {year}/
        └── ... (same structure as raw_data)
```

## 🛠️ Tech Stack

### Core Technologies
- **Python 3.8+**
- **Apache Spark** for distributed data processing
- **SQL** for data warehousing
- **Parquet** for optimized columnar storage

### Python Libraries
- **Pandas** for data manipulation
- **Dask** for parallel computing
- **PySpark** for big data processing
- **SQLAlchemy** for database interactions
- **Matplotlib/Seaborn** for data visualization

## 🚀 Getting Started

### Prerequisites
- Python 3.8 or higher
- Java 8 or higher (for Spark)
- Apache Spark 3.x
- Git

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/Climate-Trace-DataEng.git
   cd Climate-Trace-DataEng
   ```

2. **Create a virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

## 📊 Data Pipeline

### 1. Data Ingestion
- Raw data is stored in the `raw_data` directory
- Supports yearly datasets from 2021-2024
- Data is stored in Parquet format for optimal performance

### 2. Data Processing
- **Exploration**: Initial data analysis and profiling
- **Wrangling**: Data cleaning and transformation
- **Consolidation**: Combining datasets for analysis

### 3. Data Warehouse
- Dimensional modeling for analytics
- Staging and production tables
- Optimized for query performance

## 📈 Data Model

### Entity Relationship Diagram
![ERD](data_warehouse/climate_trace_erd.vsdx)

## 🧪 Running the Pipeline

1. **Run data preprocessing**
   ```bash
   python -m data_preprocessing.main
   ```

2. **Initialize data warehouse**
   ```bash
   psql -U your_username -d your_database -f data_warehouse/table_creation.sql
   ```

## 📝 License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🤝 Contributing
Contributions are welcome! Please read our [contributing guidelines](CONTRIBUTING.md) before submitting pull requests.

## 📧 Contact
For questions or feedback, please contact [Your Name] at [your.email@example.com]

---
*Last Updated: May 2025*

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
