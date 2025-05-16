import os
import pandas as pd
from report.pdf_writer import generate_pdf_from_json
import chardet

def load_data_from_directory(directory):
    # Assuming the directory contains only CSV, Parquet and JSON files
    """
    Load and return data from CSV, Parquet, and JSON files in a specified directory.

    Parameters
    ----------
    directory : str
        The path to the directory containing the data files.

    Returns
    -------
    list of tuples
        A list of tuples, each containing a pandas DataFrame and the file path 
        for each loaded file. Only files with extensions '.csv', '.parquet', 
        and '.json' (excluding those containing 'metadata' in their name) are processed.
    """
    
    data = []
    files = [file for file in os.listdir(directory) if file.endswith(('.csv', '.parquet', '.json')) and 'metadata' not in file]
    subdirectories = [name for name in os.listdir(directory) if os.path.isdir(os.path.join(directory, name))]
    
    for file in files:
        file_path = os.path.join(directory, file)
        if file.endswith('.csv'):
            with open(file_path, 'rb') as f:
                result = chardet.detect(f.read())  # or readline if the file is large
            df = pd.read_csv(file_path, engine='python', encoding=result['encoding'])
        elif file.endswith('.parquet'):
            df = pd.read_parquet(file_path)
        elif file.endswith('.json'):
            df = pd.read_json(file_path)
        data.append((df, file_path))
    
    for subdirectory in subdirectories:
        subdirectory_path = os.path.join(directory, subdirectory)
        csv_files = [file for file in os.listdir(subdirectory_path) if file.endswith('.csv')]
        parquet_files = [file for file in os.listdir(subdirectory_path) if file.endswith('.parquet')]
        json_files = [file for file in os.listdir(subdirectory_path) if file.endswith('.json') and 'metadata' not in file]
        
        for file in csv_files + parquet_files + json_files:
            file_path = os.path.join(subdirectory_path, file)
            if file.endswith('.csv'):
                with open(file_path, 'rb') as f:
                    result = chardet.detect(f.read())  # or readline if the file is large
                df = pd.read_csv(file_path, engine='python', encoding=result['encoding'])
            elif file.endswith('.parquet'):
                df = pd.read_parquet(file_path)
            elif file.endswith('.json'):
                df = pd.read_json(file_path)
            data.append((df, file_path))
    
    return data

