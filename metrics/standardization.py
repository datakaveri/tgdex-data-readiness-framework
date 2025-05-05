import re
import pandas as pd

def check_file_format(filename):
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
    return {"file_format": "Valid" if any(filename.endswith(fmt) for fmt in valid_formats) else "Invalid"}


def check_date_format(df):
    """
    Check the date format of columns in a DataFrame.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame to check.

    Returns
    -------
    dict
        A dictionary with a single key "date_format_issues" and a value of a
        list of strings, where each string is the name of a column in the
        DataFrame whose date format is not valid.
    """
    date_format_issues = []
    date_pattern = re.compile(r"\d{4}-\d{2}-\d{2}")
    for col in df.columns:
        if pd.api.types.is_string_dtype(df[col]) or pd.api.types.is_object_dtype(df[col]):
            sample = df[col].dropna().astype(str).head(10)
            if all(date_pattern.match(val) for val in sample):
                continue
            try:
                pd.to_datetime(sample, format="%Y-%m-%d")
            except:
                date_format_issues.append(col)
    return {"date_format_issues": date_format_issues,
            "date_format_issues_count": len(date_format_issues),
            "date_format_issues_percentage": round(len(date_format_issues) / df.shape[1] * 100, 2)}
