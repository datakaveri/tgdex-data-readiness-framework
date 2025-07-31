import os
def check_type_uniformity(directory):
    """
    Check whether all files in a given directory are of the same file type.

    Parameters
    ----------
    directory : str
        The path to the directory to check.

    Returns
    -------
    dict
        A dictionary with a single key "uniform_file_types" which maps to True if all files
        have the same file type, otherwise False.
    """
    label_keywords = ["label", "annotation", "groundtruth", "tag", "mask", "segmentation", "bbox", "yolo", "class", "target", "output"]
    files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f)) and not any(keyword in f.lower() for keyword in label_keywords)]
    file_types = {os.path.splitext(f)[1] for f in files}
    return {"consistency": len(file_types) <= 1}

