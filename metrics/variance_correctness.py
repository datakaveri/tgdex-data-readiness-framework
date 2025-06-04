def check_numeric_variance(df, cv_threshold=0.1):

    """
    This function takes a DataFrame and a threshold for standard deviation and 
    checks which numeric columns have a standard deviation below the threshold.

    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame to check for columns with low variance.
    std_threshold : float, optional
        The threshold for standard deviation. Defaults to 1e-1.

    Returns
    -------
    dict
        This function returns a dictionary with two keys: 'low_variance_numeric_columns' and 'percentage_low_variance_numeric_columns'. The first key has a list of column names for all numeric columns that have a standard deviation less than the threshold, and the second key has a percentage of the total number of columns that are numeric with a standard deviation less than the threshold.
    """
    numeric_cols = df.select_dtypes(include=['number'])
    if numeric_cols.empty:
        return {
            "low_variance_numeric_columns": 'None',
            "percentage_low_variance_numeric_columns": 0,
            "number_of_numeric_columns": 0,
            "numeric_columns": 'None'
        }
    low_variance_cols = []
    for col in numeric_cols:
        mean = df[col].mean()
        if mean == 0:
            continue
        if df[col].std() / mean < cv_threshold:
            low_variance_cols.append(col)
    
    return {
        "low_variance_numeric_columns": low_variance_cols,
        "percentage_low_variance_numeric_columns": round(len(low_variance_cols) / numeric_cols.shape[1] * 100, 2),
        "number_of_numeric_columns": numeric_cols.shape[1],
        "numeric_columns": numeric_cols.columns.tolist()
    }

def check_categorical_variation(df, imputed_columns=None, dominance_threshold=0.80):
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
    categorical_cols = [col for col in df.columns if imputed_columns and col in imputed_columns.get("categorical", [])]
    if not categorical_cols:
        return {
            "dominant_categorical_columns": 'None',
            "percentage_dominant_categorical_columns": 0,
            "number_of_categorical_columns": 0,
            "categorical_columns": "None"
        }

    dominant_cols = []
    for col in categorical_cols:
        if df[col].notnull().any():
            if df[col].value_counts(normalize=True).iloc[0] > dominance_threshold:
                dominant_cols.append(col)
    return {
        "dominant_categorical_columns": dominant_cols,
        "percentage_dominant_categorical_columns": round(len(dominant_cols) / len(categorical_cols) * 100, 2),
        "number_of_categorical_columns": len(categorical_cols),
        "categorical_columns": categorical_cols
    }

