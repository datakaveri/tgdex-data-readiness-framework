import os

def check_documentation_presence(descriptor_path):
    """
    Check if a documentation file exists at the given path.

    Parameters
    ----------
    descriptor_path : str or None
        The path to the file to check, or None if the file does not exist.

    Returns
    -------
    dict
        A dictionary with a single key "documentation_found" which has a value
        of True if the file exists, False otherwise.
    """
    if descriptor_path is None:
        return {"documentation_found": False}

    valid_file_names = [name.lower() for name in ["dataset_metadata", "README", "data_description", "data_description_file"]]
    valid_extensions = [ext.lower() for ext in [".txt", ".json", ".md"]]
    exists = False
    for f in valid_file_names:
        for ext in valid_extensions:
            if os.path.exists(os.path.join(descriptor_path, f + ext)):
                exists = True
                break
    return {"documentation_found": exists}

