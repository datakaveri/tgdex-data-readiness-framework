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


def check_date_format(df, imputed_columns=None):
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
    date_info = imputed_columns.get("date", {})
    columns_to_validate = date_info.get("column", [])
    expected_date_format = date_info.get("format", [])

    if not expected_date_format or not columns_to_validate:
        return {"date_column": "None", "date_issues_percentage": 'None'}

    date_fields_found = []
    date_issues_count = 0
    total_entries = 0
    if isinstance(columns_to_validate, str):
        columns_to_validate = [columns_to_validate]
    for col in columns_to_validate:
        if col not in df.columns:
            continue
        date_fields_found.append(col)
        try:
            parsed = pd.to_datetime(df[col], format=expected_date_format, errors="coerce")
            date_issues_count += parsed.isna().sum()
            total_entries += len(parsed)
        except Exception:
            continue
    return {"date_column": date_fields_found, 
            "number_of_date_columns": len(date_fields_found),
            "date_issues_percentage": round(date_issues_count / total_entries * 100, 2) if total_entries > 0 else 0.0}

