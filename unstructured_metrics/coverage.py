def region_coverage(imputed_roles):
    """
    Check if there are attributes associated with the 'region' key.

    Parameters
    ----------
    metadata : dict
        A dictionary containing metadata attributes.

    Returns
    -------
    bool
        True if the 'region' key exists and has non-empty values, False otherwise.
    """
    return {"region_coverage": bool(imputed_roles.get("regions"))}

