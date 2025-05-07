def check_coverage_region(df):
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
    region_col = [col for col in df.columns if 'district' or 'state' or 'city' or 'region' or 'subdistrict' in col.lower()]
    if not region_col:
        return {"region_coverage": "No region column found"}
    region_col = region_col[0]
    missing_percentage = df[region_col].isnull().mean() * 100
    return {"region_coverage": missing_percentage}

