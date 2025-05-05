import os
# TODO: Modify to check for schema.json

def check_documentation_presence(descriptor_path):
    """
    Check if a documentation file exists at the given path.

    Parameters
    ----------
    descriptor_path : str
        The path to the file to check.

    Returns
    -------
    dict
        A dictionary with a single key "documentation_found" which has a value
        of True if the file exists, False otherwise.
    """
    exists = os.path.exists(descriptor_path)
    return {"documentation_found": exists}
