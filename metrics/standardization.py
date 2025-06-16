import re
import pandas as pd
import os

def check_file_format(directory):
    """
    Check if the given filename has a valid file format.

    Parameters
    ----------
    filename : str
        The name of the file to check.

    Returns
    -------
    dict
        A dictionary with a single key "file_format" which maps to "Valid" if
        the file extension is one of the valid formats ('.csv', '.json', '.parquet'),
        otherwise "Invalid".
    """

    valid_formats = ['.csv', '.json', '.parquet']
    files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
    return {"file_format": "valid" if any(any(f.endswith(fmt) for fmt in valid_formats) for f in files) else "invalid"}


def check_date_and_timestamp_format(df, imputed_columns=None):
    """
    Validates date columns against the expected format specified in imputed_columns.

    Parameters
    ----------
    imputed_columns : dict
        Should contain 'date' with 'column' (list of col names) and 'format' (strftime format).
    df : pandas.DataFrame
        The dataframe to validate.

    Returns
    -------
    dict
        Column names mapped to percentage of invalid entries.
    """
    if not imputed_columns:
        return {"date_column": "None", "timestamp_column": "None", "number_of_date_columns": 0, "number_of_timestamp_columns": 0, "datetime_issues_percentage": 'None'}

    date_info = imputed_columns.get("date", {})
    timestamp_info = imputed_columns.get("timestamp", {})
    
    columns_to_validate_date = date_info.get("column") if date_info else None
    columns_to_validate_timestamp = timestamp_info.get("column") if timestamp_info else None
    expected_date_format = date_info.get("format", []) if date_info else None
    expected_timestamp_format = timestamp_info.get("format", []) if timestamp_info else None
    
    if not (expected_date_format and columns_to_validate_date) and not (expected_timestamp_format and columns_to_validate_timestamp):
        return {"date_column": "None", "timestamp_column": "None", "number_of_date_columns": 0, "number_of_timestamp_columns": 0, "datetime_issues_percentage": 'None'}
    
    date_fields_found = []
    timestamp_fields_found = []
    date_issues_count = 0
    timestamp_issues_count = 0
    total_date_entries = 0
    total_timestamp_entries = 0
    
    if isinstance(columns_to_validate_date, str):
        columns_to_validate_date = [columns_to_validate_date]
    if isinstance(columns_to_validate_timestamp, str):
        columns_to_validate_timestamp = [columns_to_validate_timestamp]

    if columns_to_validate_date is not None:
        for date_col in columns_to_validate_date:
            if date_col in df.columns and not df[date_col].isnull().all():
                date_fields_found.append(date_col)
                try:
                    parsed = pd.to_datetime(df[date_col], format=expected_date_format, errors="coerce")
                    date_issues_count += parsed.isna().sum()
                    total_date_entries += len(parsed)
                except Exception:
                    continue
    
    if columns_to_validate_timestamp is not None:
        for timestamp_col in columns_to_validate_timestamp:
            if timestamp_col in df.columns and not df[timestamp_col].isnull().all():
                timestamp_fields_found.append(timestamp_col)
                try:
                    parsed = pd.to_datetime(df[timestamp_col], format=expected_timestamp_format, errors="coerce")
                    timestamp_issues_count += parsed.isna().sum()
                    total_timestamp_entries += len(parsed)
                except Exception:
                    continue

    total_issues_count = date_issues_count + timestamp_issues_count
    total_entries = total_date_entries + total_timestamp_entries
    return {
        "date_column": date_fields_found,
        "timestamp_column": timestamp_fields_found,
        "number_of_date_columns": len(date_fields_found),
        "number_of_timestamp_columns": len(timestamp_fields_found),
        "datetime_issues_percentage": round(total_issues_count / total_entries * 100, 1) if total_entries > 0 else 0.0
    }

