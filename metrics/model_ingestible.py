def check_label_presence(df, metadata):
    """
    Check if a label column is present in the dataframe and has non-null values.

    Parameters
    ----------
    df : pandas.DataFrame
        The dataframe to check.
    metadata : dict
        A dictionary containing the label column name.

    Returns
    -------
    dict
        A dictionary with a single key "label_presence" which has a value of "OK"
        if the label column is present and has non-null values, otherwise "Missing
        or empty".
    """
    label_col = metadata.get("label_column")
    if label_col and label_col in df.columns and df[label_col].notnull().any():
        return {"label_presence": "OK"}
    return {"label_presence": "Missing or empty"}
