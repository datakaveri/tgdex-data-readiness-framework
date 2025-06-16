def check_coverage_region(df, imputed_columns=None):
    """
    Check if there is a region column in the dataframe and if it is not null.
    
    Parameters
    ----------
    df : pandas.DataFrame
        The dataframe to check.
    
    Returns
    -------
    dict
        A dictionary with a single key "region_coverage" and a value of "OK" if the
        region column is not null, "No region column found" if there is no region column,
        or "Null values present" if all values in the region column are null.
    """
    region_col = imputed_columns.get("region", []) if imputed_columns else [
        col for col in df.columns if any(keyword in col.lower() for keyword in ['district', 'state', 'city', 'region', 'subdistrict'])
    ]
    
    if not region_col:
        return {"region_coverage": 'None', "region_column": "No region column found"}
    

    missing_percentages = {}
    num_non_null_cols = 0
    overall_pct = 0
    for col in region_col:
        if col not in df.columns or df[col].isnull().all():
            continue
        missing_values = df[col].isnull().sum()
        total_values = df[col].size
        missing_percentage = (missing_values / total_values) * 100
        missing_percentages[col] = round(missing_percentage, 1)
        overall_pct += missing_percentage
        num_non_null_cols += 1
    overall_pct = round(overall_pct / num_non_null_cols, 1)
    return {"region_coverage": overall_pct, "region_column": region_col}