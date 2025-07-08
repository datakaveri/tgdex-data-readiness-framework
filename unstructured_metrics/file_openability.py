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
    return {"all_files_openable": all_openable}

# TODO: Need to handle file types that may not be readable as text. May not be feasible without knowing what file types are expected.