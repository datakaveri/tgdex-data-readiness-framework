import report.input_handler as input_handler
from report.aggregate import generate_readiness_report
import report.scoring as scoring
from report.json_writer import write_report_outputs
import pandas as pd

def main():
    df, descriptor_path, filename = input_handler.main()
    return df, descriptor_path, filename


if __name__ == "__main__":
    df, descriptor_path, filename = main()
    init_report = generate_readiness_report(df, descriptor_path, filename)
    # print(init_report)
    final_report = scoring.compute_aggregate_score(init_report, df)
    write_report_outputs(final_report, "../outputReports", init_report)
    print("Report generated successfully.")