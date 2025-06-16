print("Starting input_handler.py")
import os
import pandas as pd
import chardet
import logging
import pyarrow as pa
from pyarrow.parquet import ParquetFile
print("Importing modules completed in input_handler.py")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fix_out_of_bounds_timestamps(df):
    """
    Convert any datetime columns with out-of-bounds values to NaT.
    """
    import warnings
    min_ts = pd.Timestamp.min.value // 1000  # in microseconds
    max_ts = pd.Timestamp.max.value // 1000  # in microseconds

    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            # Already datetime, just check bounds
            s = df[col]
        elif pd.api.types.is_object_dtype(df[col]):
            # Try to convert to datetime, coerce errors
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    s = pd.to_datetime(df[col], errors='coerce')
                # For object columns, skip if conversion fails for all
                if s.notna().sum() == 0:
                    continue
            except Exception:
                continue
        else:
            continue
        # Mask out-of-bounds
        try:
            mask = (s.values.astype('int64') < pd.Timestamp.min.value) | (s.values.astype('int64') > pd.Timestamp.max.value)
            if mask.any():
                s[mask] = pd.NaT
            df[col] = s
        except Exception:
            continue
    return df

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
    metadata_names = ["dataset_metadata", "README", "data_description", "data_description_file", "data_attributes", "column_descriptor"]
    data = []
    files = [file for file in os.listdir(directory) if file.endswith(('.csv', '.parquet', '.json')) and not any(name in file for name in metadata_names) and 'metadata' not in file.lower()]
    subdirectories = [name for name in os.listdir(directory) if os.path.isdir(os.path.join(directory, name))]
    logging.info(f"Found {len(files)} files and {len(subdirectories)} subdirectories in {directory}")
    if not files and not subdirectories:
        logging.error(f"No data files found in the specified directory: {directory}")
        return []
    logging.info(f"Files found: {files}")
    logging.info(f"Subdirectories found: {subdirectories}")
    for file in files:
        file_path = os.path.join(directory, file)
        file_size = os.path.getsize(file_path)
        sample = False
        if file_size > 1*10**8:  # 100MB
            sample = True
        try:
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
                    df = fix_out_of_bounds_timestamps(df)
                    df = df.convert_dtypes()  # Convert dtypes to pandas dtypes
                    df = df.infer_objects()
                else:
                    table = pa.parquet.read_table(file_path, columns=None)
                    df = table.to_pandas(types_mapper=pd.ArrowDtype)
                    df = fix_out_of_bounds_timestamps(df)
                    df = df.convert_dtypes()  # Convert dtypes to pandas dtypes
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
            logging.info(f"Loaded file: {file_path}")
        except Exception as e:
            logging.error(f"Error loading file {file_path}: {e}")

    for subdirectory in subdirectories:
        subdirectory_path = os.path.join(directory, subdirectory)
        csv_files = [file for file in os.listdir(subdirectory_path) if file.endswith('.csv') and not any(name in file for name in metadata_names) and 'metadata' not in file.lower()]
        parquet_files = [file for file in os.listdir(subdirectory_path) if file.endswith('.parquet') and not any(name in file for name in metadata_names) and 'metadata' not in file.lower()]
        json_files = [file for file in os.listdir(subdirectory_path) if file.endswith('.json') and not any(name in file for name in metadata_names) and 'metadata' not in file.lower()]
        
        for file in csv_files + parquet_files + json_files:
            file_path = os.path.join(subdirectory_path, file)
            file_size = os.path.getsize(file_path)
            sample = False
            if file_size > 5*10**8:  # 500MB
                sample = True
            try:
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
                        df = fix_out_of_bounds_timestamps(df)
                        df = df.convert_dtypes()  # Convert dtypes to pandas dtypes
                        df = df.infer_objects()
                    else:
                        table = pa.parquet.read_table(file_path, columns=None)
                        df = table.to_pandas(types_mapper=pd.ArrowDtype)
                        df = fix_out_of_bounds_timestamps(df)
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
                logging.info(f"Loaded file: {file_path}")
            except Exception as e:
                logging.error(f"Error loading file {file_path}: {e}")
    return data

