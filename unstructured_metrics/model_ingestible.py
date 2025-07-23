import os

def check_label_presence(file_path):
    """
    Check if a file exists in the given path with a name containing any of the following words:
    "label", "annotation", "groundtruth", "tag", "mask", "segmentation", "bbox", "yolo", "class", "target", or "output"

    Parameters
    ----------
    file_path : str
        The path to check for the presence of a file.

    Returns
    -------
    bool
        True if a file is found with a name containing any of the specified words, False otherwise.
    """
    label_keywords = ["label", "annotation", "groundtruth", "tag", "mask", "segmentation", "bbox", "yolo", "class", "target", "output"]
    for filename in os.listdir(file_path):
        if any(keyword in filename.lower() for keyword in label_keywords):
            return True
    return False

