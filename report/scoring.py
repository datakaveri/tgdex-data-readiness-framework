def compute_aggregate_score(report_dict, df):
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
        "label_presence": 10,
        "timestamps_presence": 10,
        "documentation_presence": 10,
    }

    # 1. Column-wise Missing (score decreases as missing % increases)
    if "column_missing" in report_dict:
        cols = report_dict["column_missing"]
        if cols:
            avg_missing = sum(cols.values()) / len(cols)
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

    # 4. Region coverage check (Boolean)
    if "coverage_region_present" in report_dict:
        score = weights["coverage_check"] if report_dict["coverage_region_present"] else 0
        detailed_scores["coverage_check"] = score
        total_score += score

    # 5. Numeric Variance (low-variance columns)
    if "low_variance_numeric" in report_dict:
        count = report_dict["low_variance_numeric"]
        total = len(df.select_dtypes(include='number').columns)
        if total > 0:
            prop = count / total
            score = max(0, weights["numeric_variance"] * (1 - prop))
        else:
            score = weights["numeric_variance"]
        detailed_scores["numeric_variance"] = round(score, 2)
        total_score += score

    # 6. Categorical variation (dominated columns)
    if "dominant_categorical" in report_dict:
        count = report_dict["dominant_categorical"]
        total = len(df.select_dtypes(include='object').columns)
        if total > 0:
            prop = count / total
            score = max(0, weights["categorical_variation"] * (1 - prop))
        else:
            score = weights["categorical_variation"]
        detailed_scores["categorical_variation"] = round(score, 2)
        total_score += score

    # 7. File Format Check (Boolean)
    if "file_format_ok" in report_dict:
        score = weights["file_format_check"] if report_dict["file_format_ok"] else 0
        detailed_scores["file_format_check"] = score
        total_score += score

    # 8. Uniform Encoding / Date Format
    if "bad_date_columns" in report_dict:
        total = len(report_dict["bad_date_columns"]) + report_dict.get("good_date_columns_count", 0)
        if total > 0:
            prop = len(report_dict["bad_date_columns"]) / total
            score = max(0, weights["uniform_encoding"] * (1 - prop))
        else:
            score = weights["uniform_encoding"]
        detailed_scores["uniform_encoding"] = round(score, 2)
        total_score += score

    # 9. Label Presence (Boolean)
    if "label_present" in report_dict:
        score = weights["label_presence"] if report_dict["label_present"] else 0
        detailed_scores["label_presence"] = score
        total_score += score

    # 10. Timestamps Presence (Boolean)
    if "timestamps_present" in report_dict:
        score = weights["timestamps_presence"] if report_dict["timestamps_present"] else 0
        detailed_scores["timestamps_presence"] = score
        total_score += score

    # 11. Documentation Presence (Boolean)
    if "documentation_present" in report_dict:
        score = weights["documentation_presence"] if report_dict["documentation_present"] else 0
