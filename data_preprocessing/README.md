# 🌱 Climate Trace Data Preprocessing Pipeline

## 🚀 Quick Start with Google Colab

### 1. Setup Google Colab Environment

1. **Open Google Colab**
   - Go to [Google Colab](https://colab.research.google.com/)
   - Select `File` > `New notebook`

2. **Mount Google Drive**
   ```python
   from google.colab import drive
   drive.mount('/content/drive')
   ```

3. **Set Up Project Directory**
   ```python
   import os
   
   # Create project directory structure
   PROJECT_PATH = '/content/drive/MyDrive/Climate-Trace-DataEng/'
   os.makedirs(os.path.join(PROJECT_PATH, 'raw_data'), exist_ok=True)
   os.makedirs(os.path.join(PROJECT_PATH, 'processed_data'), exist_ok=True)
   ```

4. **Install Required Packages**
   ```python
   !pip install pyspark pandas numpy matplotlib seaborn
   ```

## 📁 Project Structure (Google Colab)

```
Climate-Trace-DataEng/
├── data_preprocessing/
│   ├── spark/
│   │   ├── 01_data_exploration.ipynb
│   │   ├── 02_data_wrangling.ipynb
│   │   └── 03_data_consolidation.ipynb
│   └── README.md
├── raw_data/                 # Raw data from Climate Trace
│   ├── 2021/
│   │   ├── 2021_ch4.parquet
│   │   ├── 2021_co2.parquet
│   │   ├── 2021_n2o.parquet
│   │   └── 2021_co2e_100yr.parquet
│   └── ... (2022-2024)
└── processed_data/          # Output of preprocessing
    └── ...
```

## 🔄 Data Processing Pipeline

### 1. Data Exploration (`01_data_exploration.ipynb`)

**Purpose**: Understand the raw data structure and identify data quality issues.

**Key Steps**:
- Load sample data from each year and gas type
- Check for missing values and data types
- Generate basic statistics and visualizations
- Document data quality issues

**Output**:
- Exploration report
- Data quality assessment
- Recommendations for cleaning

### 2. Data Wrangling (`02_data_wrangling.ipynb`)

**Purpose**: Clean and transform the raw data.

**Key Steps**:
- Handle missing values
- Standardize formats
- Filter relevant columns
- Apply data type conversions
- Create derived features

**Output**:
- Cleaned datasets
- Documentation of transformations
- Validation reports

### 3. Data Consolidation (`03_data_consolidation.ipynb`)

**Purpose**: Combine datasets and prepare for analysis.

**Key Steps**:
- Merge datasets by year and gas type
- Aggregate data as needed
- Create final schema
- Export to processed data directory

**Output**:
- Consolidated datasets
- Final data dictionary
- Processing logs

## 🔧 Google Colab Pro Tips

1. **Increase RAM/GPU**
   - Go to `Runtime` > `Change runtime type`
   - Select GPU/TPU if needed
   - Set RAM to High-RAM if available

2. **Save Progress**
   ```python
   # Save notebook to Google Drive
   from google.colab import files
   !cp /content/your_notebook.ipynb '/content/drive/MyDrive/Climate-Trace-DataEng/'
   ```

3. **Handle Large Datasets**
   ```python
   # Process in chunks if memory constrained
   chunk_size = 100000
   for chunk in pd.read_parquet('large_file.parquet', chunksize=chunk_size):
       # Process chunk
       process(chunk)
   ```

## 📊 Expected Output Structure

```
processed_data/
├── emissions/
│   ├── consolidated_emissions.parquet
│   └── yearly_emissions/
│       ├── 2021_emissions.parquet
│       └── ...
├── metadata/
│   ├── data_dictionary.csv
│   └── processing_logs.txt
└── analysis_ready/
    └── emissions_analysis.parquet
```

## 🛠️ Troubleshooting

**Issue**: Memory errors in Colab
- **Solution**:
  - Reduce chunk size
  - Use Dask for out-of-core processing
  - Clear unused variables: `del variable_name`

**Issue**: Drive mounting problems
- **Solution**:
  - Check authentication token
  - Remount: `drive.flush_and_unmount()` then remount

## 📝 License
This project is licensed under the MIT License.

*Last Updated: May 2025*