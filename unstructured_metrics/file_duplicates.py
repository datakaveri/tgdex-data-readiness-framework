import os

def check_file_duplicates(directory):
    """
    Check if there are any duplicate file names in a given directory.

    Parameters
    ----------
    directory : str
        The path to the directory to check.

    Returns
    -------
    dict
        A dictionary with three keys: 
        - "file_duplicates_found" which has a value of True if duplicate file names are found, False otherwise.
        - "duplicate_count" which is the number of duplicate file names.
        - "duplicate_percentage" which is the percentage of duplicate files relative to the total number of files.
    """
    files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
    file_count = len(files)
    unique_files = set(files)
    duplicate_count = file_count - len(unique_files)
    duplicate_found = duplicate_count > 0
    duplicate_percentage = (duplicate_count / file_count * 100) if file_count > 0 else 0.0

    return {
        "file_count": file_count,
        "file_duplicates_found": duplicate_found,
        "duplicate_count": duplicate_count,
        "duplicate_percentage": duplicate_percentage
    }
