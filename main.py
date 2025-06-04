from dotenv import load_dotenv
import json, os
import report.input_handler as input_handler
from report.aggregate import generate_raw_report, generate_final_report
import report.scoring as scoring
from report.multifile_average_score import calculate_average_readiness
from report.json_writer import write_report_outputs
from report.pdf_writer import generate_pdf_from_json
from metrics.llm_api import infer_column_roles_openai

# Load environment variables from .env file
load_dotenv()  
api_key = os.getenv("OPENAI_API_KEY")

def main():
    """
    Main function to run the entire data readiness report pipeline.

    This function will:

    1. Ask the user for a directory containing data files.
    2. Load all the data files in the directory.
    3. Make an API call to OpenAI to infer column roles.
    4. Run the raw readiness report for each file.
    5. Compute the aggregate score for each file.
    6. Write the raw and final reports to JSON files.
    7. Generate a PDF report for each file.
    8. If there are multiple files, generate a report with the average score across all the files.
    """
    directory = input("Enter the directory containing data files: ")

    try:
        data = input_handler.load_data_from_directory(''+directory)
        all_scores = []
        for df, file_path, sample in data:
            try:
                # Get the dataset name from the file path, strip special characters
                dataset_name = os.path.splitext(os.path.basename(file_path))[0].replace('%20', ' ').replace('%21', '!').replace('%22', '"').replace('%23', '#').replace('%24', '$').replace('%25', '%').replace('%26', '&').replace('%27', "'").replace('%28', '(').replace('%29', ')').replace('%2A', '*').replace('%2B', '+').replace('%2C', ',').replace('%2D', '-').replace('%2E', '.').replace('%2F', '/').replace('%3A', ':').replace('%3B', ';').replace('%3C', '<').replace('%3D', '=').replace('%3E', '>').replace('%3F', '?').replace('%40', '@').replace('[', '(').replace(']', ')')
                file_path = os.path.dirname(file_path)
                
                # Use OpenAI to infer column roles
                imputed_columns = infer_column_roles_openai(df, api_key)

                # Generate the raw readiness report
                init_report = generate_raw_report(df, file_path, imputed_columns)
                
                # Compute the aggregate score
                final_score = scoring.compute_aggregate_score(init_report, df)
                
                # Create a directory to hold all the generated files
                if not os.path.exists(f"outputReports/{os.path.basename(directory)}"):
                    os.makedirs(f"outputReports/{os.path.basename(directory)}")

                # Write the raw and final reports to JSON files
                write_report_outputs(final_score, f"outputReports/{os.path.basename(directory)}", dataset_name, init_report)
                final_report = generate_final_report(f"outputReports/{os.path.basename(directory)}/{dataset_name}_raw_readiness_report.json")
                with open(f"outputReports/{os.path.basename(directory)}/{dataset_name}_final_readiness_report.json", "w") as f:
                    json.dump(final_report, f, indent=4)
                print(f"Report generated for {file_path}")
                
                # Generate a PDF report
                pdf_output = f"outputReports/{os.path.basename(directory)}/{dataset_name}_data_readiness_report.pdf"
                logo_path = "plots/pretty/TGDEX_Logo Unit_Green.png"
                generate_pdf_from_json(f"outputReports/{os.path.basename(directory)}/{dataset_name}_final_readiness_report.json", pdf_output, dataset_name, final_score["total_score"], final_score["total_weights"], logo_path, sample)
                print(f"PDF generated for {file_path}")
                
                all_scores.append(final_score)
                report_names = [f"outputReports/{os.path.basename(directory)}/{dataset_name}_raw_readiness_report.json" for _, file_path, _ in data]

            except Exception as e:
                print(f"Error processing {file_path}: {e}")
                print(f"Skipping {dataset_name}")
        
        # If there are multiple files, generate a report with the average score across all the files
        if len(all_scores) > 1:
            raw_avg_report = calculate_average_readiness(report_names)

            with open("outputReports/average_score_readiness_report.json", "w") as f:
                json.dump(raw_avg_report, f, indent=4)

            final_avg_report = generate_final_report("outputReports/average_score_readiness_report.json")
            with open("outputReports/average_score_final_readiness_report.json", "w") as f:
                json.dump(final_avg_report, f, indent=4)

            pdf_output = "outputReports/average_score_data_readiness_report.pdf"
            logo_path = "plots/pretty/TGDEX_Logo Unit_Green.png"  # Set this to None if not needed
            generate_pdf_from_json("outputReports/average_score_final_readiness_report.json", pdf_output, os.path.basename(directory), raw_avg_report["total_score"], raw_avg_report["total_weights"], logo_path)
            print("Average score report generated for all datasets")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()


