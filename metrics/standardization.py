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
    return {"file_format": "Valid" if any(any(f.endswith(fmt) for fmt in valid_formats) for f in files) else "Invalid"}


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
    for col in df.select_dtypes(include=['datetime64']).columns:
        sample = df[col].dropna().head(10)
        if all(date_pattern.match(val.strftime("%Y-%m-%d")) for val in sample):
            continue
        try:
            pd.to_datetime(sample, format="%Y-%m-%d")
        except:
            date_format_issues.append(col)
    if date_format_issues:
        return {"date_format_issues": date_format_issues,
                "date_format_issues_count": len(date_format_issues),
                "date_format_issues_percentage": round(len(date_format_issues) / df.shape[0] * 100, 2)}
    else:
        return {"date_format_issues": None}

