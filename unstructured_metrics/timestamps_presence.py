def check_timestamp_presence(imputed_roles):
    """
    Check if there are any values mapped to the 'timestamp' key in the metadata.

    Parameters
    ----------
    metadata : dict
        A dictionary containing metadata attributes.

    Returns
    -------
    bool
        True if the 'timestamp' key exists and has non-empty values, False otherwise.
    """
    return {"timestamps_presence": bool(imputed_roles.get("timestamps"))}

