def check_numeric_variance(df, std_threshold=1e-4):

    """
    This function takes a DataFrame and a threshold for standard deviation and 
    checks which numeric columns have a standard deviation below the threshold.

    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame to check for columns with low variance.
    std_threshold : float, optional
        The threshold for standard deviation. Defaults to 1e-4.

    Returns
    -------
    dict
        This function returns a dictionary with two keys: 'low_variance_numeric_columns' and 'percentage_low_variance_numeric_columns'. The first key has a list of column names for all numeric columns that have a standard deviation less than the threshold, and the second key has a percentage of the total number of columns that are numeric with a standard deviation less than the threshold.
    """
    low_variance_cols = []
    for col in df.select_dtypes(include=['number']):
        if df[col].std() < std_threshold:
            low_variance_cols.append(col)
    return {"low_variance_numeric_columns": low_variance_cols,
            "percentage_low_variance_numeric_columns": round(len(low_variance_cols) / df.shape[1] * 100, 2)}

def check_categorical_variation(df, dominance_threshold=0.80):
    """
    This function takes a DataFrame and a threshold for dominance and 
    checks which categorical columns have a single category that dominates 
    more than the given threshold.

    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame to check for columns with dominating categories.
    dominance_threshold : float, optional
        The threshold for dominance. Defaults to 0.80.

    Returns
    -------
    dict
        This function returns a dictionary with two keys: 'dominant_categorical_columns' and 'percentage_dominant_categorical_columns'. The first key has a list of column names for all categorical columns that have a dominating category above the threshold, and the second key has a percentage of the total number of columns that are categorical with a dominating category above the threshold.
    """
    dominant_cols = []
    for col in df.select_dtypes(include=['object', 'category']):
        if df[col].value_counts(normalize=True).iloc[0] > dominance_threshold:
            dominant_cols.append(col)
    return {"dominant_categorical_columns": dominant_cols,
            "percentage_dominant_categorical_columns": round(len(dominant_cols) / df.shape[1] * 100, 2)}
