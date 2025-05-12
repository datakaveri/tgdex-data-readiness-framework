import report.input_handler as input_handler
from report.aggregate import generate_raw_report, generate_final_report
import report.scoring as scoring
from report.json_writer import write_report_outputs
import json, os
from report.pdf_writer import generate_pdf_from_json

def main():
    """
    Main function to run the entire data readiness report pipeline.

    This function will:

    1. Ask the user for a directory containing data files.
    2. Load all the data files in the directory.
    3. Run the raw readiness report for each file.
    4. Compute the aggregate score for each file.
    5. Write the raw and final reports to JSON files.
    6. Generate a PDF report for each file.
    """
    directory = input("Enter the directory containing data files: ")

    try:
        data = input_handler.load_data_from_directory(''+directory)
        for df, file_path in data:
            dataset_name = os.path.splitext(os.path.basename(file_path))[0].replace('%20', ' ').replace('%21', '!').replace('%22', '"').replace('%23', '#').replace('%24', '$').replace('%25', '%').replace('%26', '&').replace('%27', "'").replace('%28', '(').replace('%29', ')').replace('%2A', '*').replace('%2B', '+').replace('%2C', ',').replace('%2D', '-').replace('%2E', '.').replace('%2F', '/').replace('%3A', ':').replace('%3B', ';').replace('%3C', '<').replace('%3D', '=').replace('%3E', '>').replace('%3F', '?').replace('%40', '@').replace('[', '(').replace(']', ')')
            # Run the readiness report for each file
            init_report = generate_raw_report(df, file_path, ''+directory)
            final_score = scoring.compute_aggregate_score(init_report, df)
            write_report_outputs(final_score, "outputReports", dataset_name, init_report)
            final_report = generate_final_report(f"outputReports/{dataset_name}_raw_readiness_report.json")
            with open(f"outputReports/{dataset_name}_final_readiness_report.json", "w") as f:
                json.dump(final_report, f, indent=4)
            print(f"Report generated for {file_path}")
            pdf_output = f"outputReports/{dataset_name}_data_readiness_report.pdf"
            logo_path = "plots/pretty/TGDEX_Logo Unit_Green.png"  # Set this to None if not needed
            generate_pdf_from_json(f"outputReports/{dataset_name}_final_readiness_report.json", pdf_output, dataset_name, final_score["total_score"], logo_path)
            print(f"PDF generated for {file_path}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()