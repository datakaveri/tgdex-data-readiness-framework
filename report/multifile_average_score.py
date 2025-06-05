import json

def calculate_average_readiness(reports):
    # Define which keys to average, sum, and treat as lists
    average_keys = {'total_weights', 'total_score', 'detailed_scores', 'column_missing_percentage', 'row_missing_percentage', 'exact_row_duplicates_percentage', 'region_coverage', 'percentage_low_variance_numeric_columns', 'percentage_dominant_categorical_columns', 'datetime_issues_percentage', 'date_or_timestamp_issues_percentage'}
    sum_keys = {'column_missing_count', 'number_of_columns', 'row_missing_count', 'number_of_rows', 'exact_row_duplicates', 'number_of_numeric_columns', 'number_of_categorical_columns', 'number_of_date_columns', 'number_of_timestamp_columns'}
    list_keys = {'column_missing', 'region_column', 'low_variance_numeric_columns', 'numeric_columns', 'dominant_categorical_columns', 'categorical_columns', 'file_format', 'date_column', 'timestamp_column', 'date_or_timestamp_fields_found', 'documentation_found'}  
    average_report = {}
    count = 0

    for report_path in reports:
        with open(report_path, "r") as f:
            report = json.load(f)

        if not average_report:
            average_report = {key: 0 if key not in list_keys else [] for key in report}
        else:
            assert average_report.keys() == report.keys(), "Reports do not have the same keys"

        for key, value in report.items():
            if key in average_keys and isinstance(value, (int, float)):
                average_report[key] = (average_report[key] * count + value) / (count + 1)
            elif key in sum_keys and isinstance(value, (int, float)):
                average_report[key] += value
            elif key in list_keys and isinstance(value, list):
                # Merge lists and keep unique values
                combined = set(average_report.get(key, [])) | set(value)
                average_report[key] = list(combined)
            elif isinstance(value, dict):
                if key == 'detailed_scores':
                    if not average_report.get(key):
                        average_report[key] = value
                    else:
                        for sub_key, sub_value in value.items():
                            if sub_key in average_keys and isinstance(sub_value, (int, float)):
                                average_report[key][sub_key] = (average_report[key][sub_key] * count + sub_value) / (count + 1)
                            elif sub_key in sum_keys and isinstance(sub_value, (int, float)):
                                average_report[key][sub_key] += sub_value
                            elif sub_key in list_keys and isinstance(sub_value, list):
                                combined = set(average_report[key].get(sub_key, [])) | set(sub_value)
                                average_report[key][sub_key] = list(combined)
                            else:
                                average_report[key][sub_key] = sub_value
                else:
                    # Handle dict values recursively, if needed
                    pass
            else:
                average_report[key] = value
        count += 1

    return average_report
