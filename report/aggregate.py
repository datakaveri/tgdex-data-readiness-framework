from metrics.quality import *
from metrics.relevance_completeness import *
from metrics.variance_correctness import *
from metrics.standardization import *
from metrics.model_ingestible import *
from metrics.regular_refresh import *
from metrics.documentation import *
import json 

def generate_raw_report(df, data_file_path, imputed_columns=None):
    """
    Generate a raw data quality report from a given dataframe, descriptor path, and data directory.

    Parameters
    ----------
    df : pandas.DataFrame
        The dataframe to generate the report from.
    descriptor_path : str
        The path to the descriptor file.
    data_directory : str
        The path to the data directory.

    Returns
    -------
    dict
        A dictionary containing the raw data quality metrics.
    """
    report = {}
    report.update(check_column_missing(df))
    report.update(check_row_missing(df))
    report.update(check_row_duplicates(df))
    report.update(check_coverage_region(df, imputed_columns))
    report.update(check_numeric_variance(df))
    report.update(check_categorical_variation(df))
    report.update(check_file_format(data_file_path))
    report.update(check_date_format(df, imputed_columns))
    report.update(check_timestamp_fields(df, imputed_columns))
    report.update(check_documentation_presence(data_file_path))
    return report

def generate_final_report(readiness_metrics_json_path):
    """
    Generate a final data quality report from a given raw data quality report.

    Parameters
    ----------
    readiness_metrics_json_path : str
        The path to the raw data quality report JSON file.

    Returns
    -------
    list
        A list of dictionaries containing the final data quality report.
    """
    with open(readiness_metrics_json_path, "r") as f:
        readiness_metrics_raw = json.load(f)
    detailed_scores = readiness_metrics_raw["detailed_scores"]

    # Notes are constructed using raw metrics from the report
    def get_notes():
        """
        Construct a dictionary of notes for the final readiness report based on raw report metrics.

        Notes are strings that provide a brief description of the result of a given test. For example, if the test is 
        checking column-wise missing, the note will contain the number of columns that exceeded the missing threshold.

        Returns
        -------
        dict
            A dictionary with keys matching the raw report metrics and values as strings containing the notes.
        """
        return {
            "column_missing": f"{readiness_metrics_raw['column_missing_count']} out of {readiness_metrics_raw['number_of_columns']} columns exceed the 30% allowable threshold",
            
            "row_missing": f"{readiness_metrics_raw['row_missing_count']} out of {readiness_metrics_raw['number_of_rows']} rows exceed the 50% allowable threshold",

            "exact_row_duplicates": f"{readiness_metrics_raw['exact_row_duplicates']} duplicate rows found",
            
            "coverage_check": f"{readiness_metrics_raw['region_coverage']:.2f}% values missing in {len(readiness_metrics_raw['region_column']) if len(readiness_metrics_raw['region_column']) > 1 else readiness_metrics_raw['region_column'][0]} region column(s)" if readiness_metrics_raw['region_coverage'] != "None" else "No region column found", 
            
            "numeric_variance": f"{len(readiness_metrics_raw['low_variance_numeric_columns'])} out of {(readiness_metrics_raw['number_of_numeric_columns'])} numeric columns have low coefficient of variation",
            
            "categorical_variation": f"{len(readiness_metrics_raw['dominant_categorical_columns'])} out of {(readiness_metrics_raw['number_of_categorical_columns'])} categorical columns have dominant categories",
            
            "file_format_check": f"File format is {readiness_metrics_raw['file_format'].lower()}",
            
            "uniform_encoding": f"{readiness_metrics_raw['date_issues_percentage']} issues found in '{readiness_metrics_raw['date_column']}' column" if readiness_metrics_raw['date_issues_percentage'] != "None" else "No date column found",
                        
            "timestamp_fields_found": "All timestamp fields valid" if readiness_metrics_raw["timestamp_fields_found"] != "None" else "No timestamp fields found",
            
            "documentation_presence": "README or data dictionary file found" if readiness_metrics_raw["documentation_found"] else "No documentation file found"
        }

    notes = get_notes()

    readiness_report = [
        {
            "bucket": "Data Quality",
            "weight": 35,
            "tests": [
                {
                    "id": "1.1",
                    "key": "column_missing",
                    "title": "Column-wise Missing",
                    "note": notes["column_missing"],
                    "score": detailed_scores["column_missing"],
                    "max_score": 15
                },
                {
                    "id": "1.2",
                    "key": "row_missing",
                    "title": "Row-wise Missing",
                    "note": notes["row_missing"],
                    "score": detailed_scores["row_missing"],
                    "max_score": 10
                },
                {
                    "id": "1.3",
                    "key": "exact_row_duplicates",
                    "title": "Row Duplicates",
                    "note": notes["exact_row_duplicates"],
                    "score": detailed_scores["exact_row_duplicates"],
                    "max_score": 10
                },
            ]
        },
        {
            "bucket": "Data Relevance and Completeness",
            "weight": 0 if notes["coverage_check"] == "No region column found" else 10,
            "tests": [
                {
                    "id": "2.1",
                    "key": "coverage_check",
                    "title": "Coverage Check",
                    "note": notes["coverage_check"],
                    "score": detailed_scores["coverage_check"],
                    "max_score": 0 if notes["coverage_check"] == "No region column found" else 10
                },
            ]
        },
        {
            "bucket": "Data Variance and Correctness",
            "weight": 10,
            "tests": [
                {
                    "id": "3.1",
                    "key": "numeric_variance",
                    "title": "Numeric Variance",
                    "note": notes["numeric_variance"],
                    "score": detailed_scores["numeric_variance"],
                    "max_score": 5
                },
                {
                    "id": "3.2",
                    "key": "categorical_variation",
                    "title": "Categorical Variation",
                    "note": notes["categorical_variation"],
                    "score": detailed_scores["categorical_variation"],
                    "max_score": 5
                }
            ]
        },
        {
            "bucket": "Standardisation",
            "weight": 10 if notes["uniform_encoding"] == "No date column found" else 20,
            "tests": [
                {
                    "id": "4.1",
                    "key": "file_format_check",
                    "title": "File Format Check",
                    "note": notes["file_format_check"],
                    "score": detailed_scores["file_format_check"],
                    "max_score": 10
                },
                {
                    "id": "4.2",
                    "key": "uniform_encoding",
                    "title": "Uniform Encoding of Dates",
                    "note": notes["uniform_encoding"],
                    "score": detailed_scores["uniform_encoding"],
                    "max_score": 0 if notes["uniform_encoding"] == "No date column found" else 10
                }
            ]
        },
        {
            "bucket": "Regular Refresh",
            "weight": 0 if notes["timestamp_fields_found"] == "No timestamp fields found" else 10,
            "tests": [
                {
                    "id": "5.1",
                    "key": "timestamp_fields_found",
                    "title": "Timestamps Presence",
                    "note": notes["timestamp_fields_found"],
                    "score": detailed_scores["timestamp_fields_found"],
                    "max_score": 0 if notes["timestamp_fields_found"] == "No timestamp fields found" else 10
                }
            ]
        },
        {
            "bucket": "Documentation",
            "weight": 15,
            "tests": [
                {
                    "id": "6.1",
                    "key": "documentation_presence",
                    "title": "Documentation Presence",
                    "note": notes["documentation_presence"],
                    "score": detailed_scores["documentation_presence"],
                    "max_score": 15
                }
            ]
        }
    ]

    return readiness_report