from metrics.quality import *
from metrics.relevance_completeness import *
from metrics.variance_correctness import *
from metrics.standardization import *
from metrics.model_ingestible import *
from metrics.regular_refresh import *
from metrics.documentation import *

def generate_readiness_report(df, descriptor_path, data_directory):
    report = {}
    report.update(check_column_missing(df))
    report.update(check_row_missing(df))
    report.update(check_row_duplicates(df))
    report.update(check_coverage_region(df))
    report.update(check_numeric_variance(df))
    report.update(check_categorical_variation(df))
    report.update(check_file_format(data_directory))
    report.update(check_date_format(df))
    report.update(check_label_presence(df))
    report.update(check_timestamp_fields(df))
    report.update(check_documentation_presence(descriptor_path))
    return report

