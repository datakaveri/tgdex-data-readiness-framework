import pandas as pd

def check_timestamp_fields(df,imputed_columns=None):
    """
    Validates timestamp columns against the expected format specified in imputed_columns.

    Parameters
    ----------
    imputed_columns : dict
        Should contain 'timestamp' with 'column' (list of col names) and 'format' (strftime format).
    df : pandas.DataFrame
        The dataframe to validate.

    Returns
    -------
    dict
        Column names mapped to percentage of invalid entries.
    """
    if imputed_columns is None:
        return {"timestamp_fields_found": 'None',
            "timestamp_issues_percentage": 'None'}
    timestamp_info = imputed_columns.get("timestamp", {})
    timestamp_cols = timestamp_info.get("column", [])
    timestamp_format = timestamp_info.get("format")

    if not timestamp_format or not timestamp_cols:
        return {"timestamp_fields_found": 'None',
            "timestamp_issues_percentage": 'None'}

    timestamp_fields_found = []
    overall_issues = 0
    for col in timestamp_cols:
        if col not in df.columns:
            continue
        timestamp_fields_found.append(col)
        try:
            parsed = pd.to_datetime(df[col], format=timestamp_format, errors="coerce")
            overall_issues += parsed.isna().sum()
        except Exception:
            continue
    if not timestamp_fields_found:
        return {"timestamp_fields_found": timestamp_fields_found,
                "timestamp_issues_percentage": 'None'}
    overall_pct = round(overall_issues / df.shape[0] * 100, 2)
    return {"timestamp_fields_found": timestamp_fields_found,
            "timestamp_issues_percentage": overall_pct}

