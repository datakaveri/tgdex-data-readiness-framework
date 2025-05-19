def check_label_presence(df, imputed_columns=None):
    """
    Check if a label column is present in the dataframe and has non-null values.

    Parameters
    ----------
    df : pandas.DataFrame
        The dataframe to check.
    imputed_columns : dict, optional
        A dictionary containing the imputed column names and their inferred types.

    Returns
    -------
    dict
        A dictionary with a single key "label_presence" which has a value of the
        percentage of rows with non-null values in the label column if the label 
        column is present, otherwise "Missing or empty".
    """
    if imputed_columns is None:
        return {"label_presence_count": "None", "label_column": "Label column not found"}

    label_col = imputed_columns.get("label")
    if label_col and label_col in df.columns:
        if df[label_col].empty:
            return {"label_presence_count": "None", "label_column": "Label column not found"}
        non_null_percentage = df[label_col].notnull().mean() * 100
        return {"label_presence_count": f"{non_null_percentage:.2f}", "label_column": label_col}
    return {"label_presence_count": "None", "label_column": "Label column not found"}

