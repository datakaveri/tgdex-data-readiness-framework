import os

def check_file_openability(directory):
    """
    Check if all the files in a given directory are openable without errors.

    Parameters
    ----------
    directory : str
        The path to the directory to check.

    Returns
    -------
    dict
        A dictionary with a single key "all_files_openable" which has a value of
        True if all the files in the directory are openable without errors, False
        otherwise.
    """
    files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
    sample = files[:min(1000, len(files))]
    all_openable = True
    for file in sample:
        file_path = os.path.join(directory, file)
        try:
            with open(file_path, 'r') as f:
                f.read()
        except IOError:
            all_openable = False
            break
    not_openable_count = len(files) - sample.count(True)
    not_openable_percentage = not_openable_count / len(files) * 100
    openable_count = len(files) - not_openable_count
    openable_percentage = 100 - not_openable_percentage
    return {"not_openable_count": not_openable_count, 
            "not_openable_percentage": not_openable_percentage, 
            "openable_count": openable_count, 
            "openable_percentage": openable_percentage}

