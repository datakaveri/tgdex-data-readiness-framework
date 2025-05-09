import os
import pandas as pd
from report.pdf_writer import generate_pdf_from_json

def load_data_from_directory(directory):
    # Assuming the directory contains only CSV, Parquet and JSON files
    csv_files = [file for file in os.listdir(directory) if file.endswith('.csv')]
    parquet_files = [file for file in os.listdir(directory) if file.endswith('.parquet')]
    json_files = [file for file in os.listdir(directory) if file.endswith('.json') and 'metadata' not in file]
    
    data = []
    for file in csv_files + parquet_files + json_files:
        file_path = os.path.join(directory, file)
        if file.endswith('.csv'):
            df = pd.read_csv(file_path)
        elif file.endswith('.parquet'):
            df = pd.read_parquet(file_path)
        elif file.endswith('.json'):
            df = pd.read_json(file_path)
        data.append((df, file_path))
    
    return data

