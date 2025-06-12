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

    valid_file_names = [name.lower() for name in ["dataset_metadata", "README", "data_description", "data_description_file", "data_attributes"]]
    valid_extensions = [ext.lower() for ext in [".txt", ".json", ".md", ".csv"]]
    for filename in os.listdir(descriptor_path):
        name, ext = os.path.splitext(filename)
        name_lower = name.lower()
        ext_lower = ext.lower()

        if ext_lower in valid_extensions:
            if name_lower in valid_file_names or "metadata" in name_lower:
                return {"documentation_found": True}
    return {"documentation_found": False}