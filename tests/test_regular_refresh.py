# tests/test_regular_refresh.py
import pandas as pd
from structured_metrics.regular_refresh import check_date_or_timestamp_fields

def test_check_date_or_timestamp_fields_empty_df():
    df = pd.DataFrame()
    result = check_date_or_timestamp_fields(df)
    expected = {"date_or_timestamp_fields_found": 'None',
                "date_or_timestamp_issues_percentage": 'None'}
    assert result == expected

def test_check_date_or_timestamp_fields_no_imputed_columns():
    df = pd.DataFrame({'date': [1, 2, 3]})
    result = check_date_or_timestamp_fields(df)
    expected = {"date_or_timestamp_fields_found": 'None',
                "date_or_timestamp_issues_percentage": 'None'}
    assert result == expected

def test_check_date_or_timestamp_fields_with_imputed_columns():
    df = pd.DataFrame({'date': [1, 2, 3]})
    imputed_columns = {'date': {'column': ['date'], 'format': '%Y-%m-%d'}}
    result = check_date_or_timestamp_fields(df, imputed_columns)
    expected = {"date_or_timestamp_fields_found": ['date'],
                "date_or_timestamp_issues_percentage": 100.0}
    assert result == expected

def test_check_date_or_timestamp_fields_with_missing_values():
    df = pd.DataFrame({'date': [1, None, 3]})
    imputed_columns = {'date': {'column': ['date'], 'format': '%Y-%m-%d'}}
    result = check_date_or_timestamp_fields(df, imputed_columns)
    expected = {"date_or_timestamp_fields_found": ['date'],
                "date_or_timestamp_issues_percentage": 66.67}
    assert result == expected

def test_check_date_or_timestamp_fields_with_multiple_columns():
    df = pd.DataFrame({'date': [1, 2, 3], 'timestamp': [4, 5, 6]})
    imputed_columns = {'date': {'column': ['date'], 'format': '%Y-%m-%d'},
                       'timestamp': {'column': ['timestamp'], 'format': '%Y-%m-%d %H:%M:%S'}}
    result = check_date_or_timestamp_fields(df, imputed_columns)
    expected = {"date_or_timestamp_fields_found": ['date', 'timestamp'],
                "date_or_timestamp_issues_percentage": 100.0}
    assert result == expected

def test_check_date_or_timestamp_fields_with_only_timestamp_column():
    df = pd.DataFrame({'timestamp': ['2022-01-01 00:00:00', None, '']})
    imputed_columns = {'timestamp': {'column': ['timestamp'], 'format': '%Y-%m-%d %H:%M:%S'}}
    result = check_date_or_timestamp_fields(df, imputed_columns)
    expected = {"date_or_timestamp_fields_found": ['timestamp'],
                "date_or_timestamp_issues_percentage": 66.67}
    assert result == expected
