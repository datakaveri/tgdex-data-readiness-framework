import os
import pandas as pd
import chardet
import pyarrow as pa
from pyarrow.parquet import ParquetFile

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
        file_size = os.path.getsize(file_path)
        sample = False
        if file_size > 1*10**8:  # 100MB
            sample = True
        
        if file.endswith('.csv'):
            with open(file_path, 'rb') as f:
                # Avoid reading large files
                # chunk = f.read(10000)
                # result = chardet.detect(chunk)
                # encoding = result['encoding']
                # if encoding is not None and encoding.lower() == 'ascii':
                encoding = 'utf-8'
            if sample:
                df = pd.read_csv(file_path, engine='python', encoding=encoding, nrows=1000000)
                df = df.infer_objects()  # Convert dtypes to pandas dtypes
            else:
                df = pd.read_csv(file_path, engine='python', encoding=encoding)
                df = df.infer_objects()  # Convert dtypes to pandas dtypes
        elif file.endswith('.parquet'):
            if sample:
                pf = ParquetFile(file_path)
                schema = pf.schema_arrow
                first_batch = next(pf.iter_batches(batch_size=1000000))
                df = pa.Table.from_batches([first_batch], schema=schema).to_pandas()
                df = df.convert_dtypes()  # Convert dtypes to pandas dtypes
                df = df.infer_objects()
            else:
                table = pa.parquet.read_table(file_path, columns=None)
                df = table.to_pandas(types_mapper=pd.ArrowDtype)
                df = df.convert_dtypes(dtype_backend='pyarrow')  # Convert dtypes to pandas dtypes
                df = df.infer_objects()
        elif file.endswith('.json'):
            if sample:
                df = pd.read_json(file_path, chunksize=1000000)
                df = pd.concat([chunk for chunk in df])
                df = df.infer_objects()  # Convert dtypes to pandas dtypes
            else:
                df = pd.read_json(file_path)
                df = df.infer_objects()  # Convert dtypes to pandas dtypes
        data.append((df, file_path, sample))

    for subdirectory in subdirectories:
        subdirectory_path = os.path.join(directory, subdirectory)
        csv_files = [file for file in os.listdir(subdirectory_path) if file.endswith('.csv')]
        parquet_files = [file for file in os.listdir(subdirectory_path) if file.endswith('.parquet')]
        json_files = [file for file in os.listdir(subdirectory_path) if file.endswith('.json') and 'metadata' not in file]
        
        for file in csv_files + parquet_files + json_files:
            file_path = os.path.join(subdirectory_path, file)
            file_size = os.path.getsize(file_path)
            sample = False
            if file_size > 5*10**8:  # 500MB
                sample = True
            
            if file.endswith('.csv'):
                with open(file_path, 'rb') as f:
                    chunk = f.read(10000)
                    result = chardet.detect(chunk)
                    encoding = result['encoding']
                if sample:
                    df = pd.read_csv(file_path, engine='python', encoding=encoding, nrows=1000000)
                    df = df.infer_objects()  # Convert dtypes to pandas dtypes
                else:
                    df = pd.read_csv(file_path, engine='python', encoding=encoding)
                    df = df.infer_objects()  # Convert dtypes to pandas dtypes
            elif file.endswith('.parquet'):
                if sample:
                    pf = ParquetFile(file_path)
                    schema = pf.schema_arrow
                    first_batch = next(pf.iter_batches(batch_size=1000000))
                    df = pa.Table.from_batches([first_batch], schema=schema).to_pandas()
                    df = df.convert_dtypes()  # Convert dtypes to pandas dtypes
                    df = df.infer_objects()
                else:
                    table = pa.parquet.read_table(file_path, columns=None)
                    df = table.to_pandas(types_mapper=pd.ArrowDtype)
                    df = df.convert_dtypes(dtype_backend='pyarrow')  # Convert dtypes to pandas dtypes
                    df = df.infer_objects()
            elif file.endswith('.json'):
                if sample:
                    df = pd.read_json(file_path, chunksize=1000000)
                    df = pd.concat([chunk for chunk in df])
                    df = df.infer_objects()  # Convert dtypes to pandas dtypes
                else:
                    df = pd.read_json(file_path)
                    df = df.infer_objects()  # Convert dtypes to pandas dtypes
            data.append((df, file_path, sample))
    
    return data

