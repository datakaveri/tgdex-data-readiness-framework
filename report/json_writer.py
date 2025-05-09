import os
import json
# from report.aggregate import generate_readiness_report
def write_report_outputs(report, out_path, dataset_name, additional_report=None):
    os.makedirs(out_path, exist_ok=True)
    filename = f"{dataset_name}_raw_readiness_report.json"
    json_path = os.path.join(out_path, filename)

    output = {**report, **additional_report}
    with open(json_path, "w") as f:
        json.dump(output, f, indent=4)

