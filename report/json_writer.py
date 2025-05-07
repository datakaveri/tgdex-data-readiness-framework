import os
import json
from report.aggregate import generate_readiness_report
def write_report_outputs(report, out_path, additional_report=None):
    os.makedirs(out_path, exist_ok=True)
    json_path = os.path.join(out_path, "readiness_report.json")

    output = {**report, **additional_report}
    with open(json_path, "w") as f:
        json.dump(output, f, indent=4)

