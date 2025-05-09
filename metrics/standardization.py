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
    date_patterns = {
        "%Y-%m-%d": re.compile(r"\d{4}-\d{2}-\d{2}"),
        "%d/%m/%Y": re.compile(r"\d{2}/\d{2}/\d{4}"),
        "%m-%d-%Y": re.compile(r"\d{2}-\d{2}-\d{4}"),
        "%Y-%m-%dT%H:%M:%S%z": re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+\d{2}:\d{2}"),
        "%Y-%m-%dT%H:%M:%S.%f%z": re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{1,6}\+\d{2}:\d{2}"),
        "%Y-%m-%dT%H:%M:%SZ": re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z"),
        "%Y-%m-%dT%H:%M:%S": re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"),
        "%Y-%m-%dT%H:%M:%S.%f": re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{1,6}"),
        "%m/%d/%Y %H:%M:%S": re.compile(r"\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}:\d{2}"),
        "%m/%d/%Y %H:%M:%S %z": re.compile(r"\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}:\d{2} [+-]\d{4}"),
    }
    
    for col in df.select_dtypes(include=['datetime64']).columns:
        sample = df[col].dropna()
        if sample.shape[0] > 100000:
            sample = sample.head(100)
        
        for date_format, pattern in date_patterns.items():
            if all(pattern.match(val.strftime(date_format)) for val in sample):
                break
        else:
            try:
                pd.to_datetime(sample, errors='raise')
            except:
                date_format_issues.append(col)
    
    if date_format_issues:
        return {
            "date_format_issues": date_format_issues,
            "date_format_issues_count": len(date_format_issues),
            "date_format_issues_percentage": round(len(date_format_issues) / df.shape[0] * 100, 2)
        }
    else:
        return {"date_format_issues": None}

