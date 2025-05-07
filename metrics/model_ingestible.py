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
        A dictionary with a single key "label_presence" which has a value of the
        percentage of rows with non-null values in the label column if the label 
        column is present, otherwise "Missing or empty".
    """
    label_col = metadata.get("label_column")
    if label_col and label_col in df.columns:
        non_null_percentage = df[label_col].notnull().mean() * 100
        return {"label_presence": f"{non_null_percentage:.2f}%"}
    return {"label_presence": None}
