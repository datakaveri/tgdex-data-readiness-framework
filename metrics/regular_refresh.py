import pandas as pd

def check_date_or_timestamp_fields(df, imputed_columns=None):
    """
    Checks the fill rate of date and/or timestamp columns.

    Parameters
    ----------
    imputed_columns : dict
        Should contain 'date' and 'timestamp' with 'column' (list of col names) and 'format' (strftime format).
    df : pandas.DataFrame
        The dataframe to validate.

    Returns
    -------
    dict
        Column names mapped to percentage of non-null entries.
    """
    if imputed_columns is None:
        return {"date_or_timestamp_fields_found": 'None',
            "date_or_timestamp_issues_percentage": 'None'}
    
    date_info = imputed_columns.get("date", {})
    timestamp_info = imputed_columns.get("timestamp", {})
    
    date_cols = date_info.get("column", []) if date_info else None
    if isinstance(date_cols, str):
        date_cols = [date_cols]
    
    timestamp_cols = timestamp_info.get("column", []) if timestamp_info else None
    if isinstance(timestamp_cols, str):
        timestamp_cols = [timestamp_cols]
   
    columns_to_validate = []
    if date_cols:
        columns_to_validate += date_cols
    if timestamp_cols:
        columns_to_validate += timestamp_cols
    if not columns_to_validate:
        return {"date_or_timestamp_fields_found": 'None',
            "date_or_timestamp_issues_percentage": 'None'}
    
    date_or_timestamp_fields_found = []
    overall_pct = 0
    
    for col in columns_to_validate:
        if col not in df.columns:
            continue
        date_or_timestamp_fields_found.append(col)
        overall_pct += df[col].isnull().mean() * 100
    overall_pct = round(overall_pct / len(date_or_timestamp_fields_found), 2)
    return {"date_or_timestamp_fields_found": date_or_timestamp_fields_found,
            "date_or_timestamp_issues_percentage": overall_pct}


