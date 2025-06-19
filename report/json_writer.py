import os
import json
# from report.aggregate import generate_readiness_report
def write_report_outputs(report, out_path, dataset_name, additional_report=None):
    """
    Write the report outputs to a JSON file.

    Parameters
    ----------
    report : dict
        The main report data to be written to the JSON file.
    out_path : str
        The directory path where the JSON file will be saved.
    dataset_name : str
        The name of the dataset, used for naming the JSON file.
    additional_report : dict, optional
        Additional data to be included in the JSON file, by default None.

    Returns
    -------
    None
    """

    os.makedirs(out_path, exist_ok=True)
    filename = f"{dataset_name}_raw_readiness_report.json"
    json_path = os.path.join(out_path, filename)

    output = {**report, **additional_report}
    with open(json_path, "w") as f:
        json.dump(output, f, indent=4)

