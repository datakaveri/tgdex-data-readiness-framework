def compute_aggregate_score(report_dict, df):
    """
    Computes the aggregate score from a dictionary of individual metrics.

    Parameters
    ----------
    report_dict : dict
        A dictionary of individual metrics, each with a score out of 100
    df : pandas.DataFrame
        The DataFrame containing the dataset

    Returns
    -------
    final_report : dict
        A dictionary with the total score and detailed scores for each metric
    """
    total_score = 0
    detailed_scores = {}

    # Define metric weights (out of 100)
    weights = {
        "column_missing": 15,
        "row_missing": 10,
        "exact_row_duplicates": 10,
        "coverage_check": 10,
        "numeric_variance": 5,
        "categorical_variation": 5,
        "file_format_check": 5,
        "uniform_encoding": 10,
        # "label_presence": 10,
        "timestamp_fields_found": 10,
        "documentation_presence": 10,
    }

    # 1. Column-wise Missing (score decreases as missing % increases)
    if "column_missing" in report_dict:
        cols = report_dict["column_missing"]
        if "column_missing_count" in report_dict and "number_of_columns" in report_dict:
            missing_cols = report_dict["column_missing_count"]
            total_cols = report_dict["number_of_columns"]
            avg_missing = (missing_cols / total_cols) * 100
            score = max(0, weights["column_missing"] * (1 - avg_missing / 100))
        else:
            score = weights["column_missing"]
        detailed_scores["column_missing"] = round(score, 2)
        total_score += score

    # 2. Row-wise Missing (proportion of rows with >50% missing)
    if "row_missing_count" in report_dict:
        affected_rows = report_dict["row_missing_count"]
        prop = affected_rows / len(df)
        score = max(0, weights["row_missing"] * (1 - prop))
        detailed_scores["row_missing"] = round(score, 2)
        total_score += score

    # 3. Exact Row Duplicates
    if "exact_row_duplicates" in report_dict:
        dupes = report_dict["exact_row_duplicates"]
        prop = dupes / len(df)
        score = max(0, weights["exact_row_duplicates"] * (1 - prop))
        detailed_scores["exact_row_duplicates"] = round(score, 2)
        total_score += score

    # 4. Region coverage check (Based on percentage of missing values)
    if "region_coverage" in report_dict:
        missing_percentage = report_dict["region_coverage"]
        region_column = report_dict["region_column"]
        if missing_percentage == 'None':
            score = weights["coverage_check"]
        else:
            score = max(0, weights["coverage_check"] * (1 - missing_percentage / 100))
        detailed_scores["coverage_check"] = round(score, 2)
        total_score += score

    # 5. Numeric Variance (low-variance columns)
    if "low_variance_numeric_columns" in report_dict:
        percent = report_dict["percentage_low_variance_numeric_columns"]
        if percent > 0:
            score = max(0, weights["numeric_variance"] * (1 - percent / 100))
        else:
            score = weights["numeric_variance"]
        detailed_scores["numeric_variance"] = round(score, 2)
        total_score += score

    # 6. Categorical variation (dominated columns)
    if "dominant_categorical_columns" in report_dict:
        percent = report_dict["percentage_dominant_categorical_columns"]
        if percent > 0:  
            score = max(0, weights["categorical_variation"] * (1 - percent / 100))
        else:
            score = weights["categorical_variation"]
        detailed_scores["categorical_variation"] = round(score, 2)
        total_score += score

    # 7. File Format Check (Boolean)
    if "file_format" in report_dict:
        score = weights["file_format_check"] if report_dict["file_format"] == "valid" else 0

        detailed_scores["file_format_check"] = score
        total_score += score

    # 8. Uniform Encoding / Date Format
    if "date_issues_percentage" in report_dict:
        if report_dict["date_issues_percentage"] != "None":
            prop = report_dict["date_issues_percentage"]
            score = max(0, weights["uniform_encoding"] * (1 - prop / 100))
        else:
            score = weights["uniform_encoding"]
        detailed_scores["uniform_encoding"] = round(score, 2)
        total_score += score

    # # 9. Label Presence (Boolean)
    # if "label_presence_count" in report_dict:
    #     if report_dict["label_presence_count"] == 'None':
    #         score = weights["label_presence"]
    #     else:
    #         non_null_percentage = float(report_dict["label_presence_count"])
    #         score = max(0, weights["label_presence"] * non_null_percentage / 100)
    #     detailed_scores["label_presence"] = round(score, 2)
    #     total_score += score

    # 10. Timestamps Presence (Boolean)
    if "timestamp_fields_found" in report_dict:
        if report_dict["timestamp_fields_found"] == 'None':
            score = weights["timestamp_fields_found"]
        else:
            prop = report_dict["timestamp_issues_percentage"]
            score = max(0, weights["timestamp_issues_percentage"] * (1 - prop / 100))
        detailed_scores["timestamp_fields_found"] = round(score, 2)
        total_score += score

    # 11. Documentation Presence (Boolean)
    if "documentation_found" in report_dict:
        score = weights["documentation_presence"] if report_dict["documentation_found"] else 0

        detailed_scores["documentation_presence"] = score
        total_score += score
    
    # Output the results as a dictionary
    final_report = {
        "total_score": round(total_score, 2),
        "detailed_scores": detailed_scores
    }
    
    return final_report

