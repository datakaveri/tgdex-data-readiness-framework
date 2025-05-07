import pandas as pd

def check_timestamp_fields(df):
    """
    Check which columns in the DataFrame have names that match the
    timestamp_column entries in the metadata dictionary.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame to check for timestamp columns.
    metadata : dict
        A dictionary containing the timestamp_columns key with a list of
        column names as value.

    Returns
    -------
    dict
        A dictionary with a single key "timestamp_fields_found" which maps
        to a list of column names that are present in the DataFrame.
    """
    timestamps = ["created_at", "updated_at"]
    present = []
    for col in timestamps:
        if col in df.columns and pd.api.types.is_datetime64_any_dtype(df[col]):
            present.append(col)
    if not present:
        return {"timestamp_fields_found": "None"}
    return {"timestamp_fields_found": present}

