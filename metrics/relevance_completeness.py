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
    
    missing_percentage = df[region_col].isnull().stack().mean() * 100
    return {"region_coverage": missing_percentage, "region_column": region_col}

