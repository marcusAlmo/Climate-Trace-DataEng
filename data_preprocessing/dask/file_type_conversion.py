import pandas as pd
import dask.dataframe as dd
import os

def convert_to_csv(file_path, num_workers=4):
    try:
        print(f"Converting {file_path} to CSV using Dask...")
        ddf = dd.read_parquet(file_path, engine='pyarrow')
        csv_path = file_path.replace('.parquet', '.csv')
        
        # Use Dask's parallel write with specified number of workers
        ddf.to_csv(csv_path, index=False, single_file=True, compute=True, num_workers=num_workers)
        print(f"Successfully converted {file_path} to CSV.")
    except Exception as e:
        print(f"Error converting {file_path} to CSV: {e}")

def convert_to_xlsx(file_path, num_workers=4):
    try:
        print(f"Converting {file_path} to XLSX using Dask...")
        ddf = dd.read_parquet(file_path, engine='pyarrow')
        xlsx_path = file_path.replace('.parquet', '.xlsx')
        
        # Convert Dask DataFrame to Pandas for Excel writing
        df = ddf.compute()
        df.to_excel(xlsx_path, index=False)
        print(f"Successfully converted {file_path} to XLSX.")
    except Exception as e:
        print(f"Error converting {file_path} to XLSX: {e}")

def convert_file_types(conversion_type):
    file_name = input("Enter the target file name: ")
    directory = input("Enter the directory path: ")
    target = os.path.join(directory, f'{file_name}.parquet')

    # Optional: Allow user to specify number of workers
    try:
        num_workers = int(input("Enter number of workers (default is 4): ") or 4)
    except ValueError:
        num_workers = 4

    if conversion_type == 'csv':
        convert_to_csv(target, num_workers)
    elif conversion_type == 'xlsx':
        convert_to_xlsx(target, num_workers)

if __name__ == '__main__':
    print("Press ctrl + c to exit.")
    while True:
        try:
            conversion_type = input("Enter the type of file to convert (csv or xlsx): ")
            if conversion_type in ['csv', 'xlsx']:
                convert_file_types(conversion_type)
            else:
                print("Invalid file type. Please enter 'csv' or 'xlsx'.")
        except KeyboardInterrupt:
            print("\nExiting...")
            break