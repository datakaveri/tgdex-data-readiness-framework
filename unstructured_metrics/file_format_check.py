import os

def check_file_format(directory):
    """
    Check if all the files in a given directory are in a standard format.

    Parameters
    ----------
    directory : str
        The path to the directory to check.

    Returns
    -------
    dict
        A dictionary with a single key "file_format" which has a value of
        "Valid" if all the files in the directory are in a standard format,
        otherwise "Invalid".
    """
    valid_formats = ['.xlsx', '.xls', '.pdf', '.mp3', '.jpg', '.png', '.tiff', '.tif', '.txt', '.md', '.dcm']
    files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
    non_valid_format_count = sum(1 for f in files if not any(f.endswith(fmt) for fmt in valid_formats))
    non_valid_format_percentage = (non_valid_format_count / len(files)) * 100
    return {"deviant_count": non_valid_format_count, "deviant_percentage": non_valid_format_percentage}

