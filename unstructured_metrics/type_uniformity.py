import os
def check_type_uniformity(directory):
    """
    Check the percentage of files in a given directory that deviate from the most common file type.

    Parameters
    ----------
    directory : str
        The path to the directory to check.

    Returns
    -------
    dict
        A dictionary with a single key "deviation_percentage" which maps to the percentage
        of files that deviate from the most common file type.
    """
    files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
    file_types = [os.path.splitext(f)[1] for f in files]
    
    if not file_types:
        return {"deviation_percentage": 0.0}

    most_common_type = max(set(file_types), key=file_types.count)
    deviation_count = sum(1 for f in file_types if f != most_common_type)
    deviation_percentage = (deviation_count / len(file_types)) * 100

    return {"most_common_type": most_common_type,
            "total_files": len(file_types),
            "deviation_count": deviation_count,
            "deviation_percentage": deviation_percentage}
