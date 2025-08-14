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
        A dictionary with the following keys:
        - "valid_format_count": The number of files in the directory that are in a standard format.
        - "invalid_format_count": The number of files in the directory that are not in a standard format.
        - "valid_format_percentage": The percentage of files in the directory that are in a standard format.
        - "invalid_format_percentage": The percentage of files in the directory that are not in a standard format.
    """
    valid_formats = ['.xlsx', '.xls', '.pdf', '.mp3', '.jpg', '.jpeg', '.png', '.tiff', '.tif', '.txt', '.md', '.dcm']
    label_keywords = ["label", "annotation", "groundtruth", "tag", "mask", "segmentation", "bbox", "yolo", "class", "target", "output"]
    files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f)) and not any(keyword in f.lower() for keyword in label_keywords)]
    valid_format_count = sum(1 for f in files if any(f.endswith(fmt) for fmt in valid_formats))
    invalid_format_count = len(files) - valid_format_count
    valid_format_percentage = (valid_format_count / len(files)) * 100 if files else 0.0
    invalid_format_percentage = 100 - valid_format_percentage
    return {"valid_format_count": valid_format_count, 
            "invalid_format_count": invalid_format_count,
            "valid_format_percentage": valid_format_percentage,
            "invalid_format_percentage": invalid_format_percentage}

