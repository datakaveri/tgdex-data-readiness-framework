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
    valid_formats = ['.jpg', '.png', '.wav', '.mp3', '.txt', '.csv', '.json', '.pdf', '.docx', '.xlsx', '.pptx', '.html', '.xml','.dicom']
    files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
    return {"file_format": "Valid" if all(any(f.endswith(fmt) for fmt in valid_formats) for f in files) else "Invalid"}
