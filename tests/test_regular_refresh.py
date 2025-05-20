# tests/test_regular_refresh.py
import pandas as pd
from metrics.regular_refresh import check_timestamp_fields

def test_check_timestamp_fields_empty_df():
    df = pd.DataFrame()
    imputed_columns = {'timestamp': {'column': ['date'], 'format': '%Y-%m-%d'}}
    result = check_timestamp_fields(df, imputed_columns)
    expected = {'timestamp_fields_found': [], 'timestamp_issues_percentage': 'None'}
    assert result == expected

def test_check_timestamp_fields_no_timestamp_column():
    data = {'A': [1, 2, 3, 4], 'B': [1, 2, 3, 4]}
    df = pd.DataFrame(data)
    imputed_columns = {'timestamp': {'column': ['date'], 'format': '%Y-%m-%d'}}
    result = check_timestamp_fields(df, imputed_columns)
    expected = {'timestamp_fields_found': [], 'timestamp_issues_percentage': 'None'}
    assert result == expected

def test_check_timestamp_fields_valid_timestamp():
    data = {'date': ['2022-01-01', '2022-01-02', '2022-01-03', '2022-01-04']}
    df = pd.DataFrame(data)
    imputed_columns = {'timestamp': {'column': ['date'], 'format': '%Y-%m-%d'}}
    result = check_timestamp_fields(df, imputed_columns)
    expected = {'timestamp_fields_found': ['date'], 'timestamp_issues_percentage': 0.0}
    assert result == expected

def test_check_timestamp_fields_invalid_timestamp():
    data = {'date': ['2022-01-01', 'invalid', '2022-01-03', '2022-01-04']}
    df = pd.DataFrame(data)
    imputed_columns = {'timestamp': {'column': ['date'], 'format': '%Y-%m-%d'}}
    result = check_timestamp_fields(df, imputed_columns)
    expected = {'timestamp_fields_found': ['date'], 'timestamp_issues_percentage': 25.0}
    assert result == expected

def test_check_timestamp_fields_multiple_timestamp_columns():
    data = {'date1': ['2022-01-01', '2022-01-02', '2022-01-03', '2022-01-04'],
            'date2': ['2022-01-01', 'invalid', '2022-01-03', '2022-01-04']}
    df = pd.DataFrame(data)
    imputed_columns = {'timestamp': {'column': ['date1', 'date2'], 'format': '%Y-%m-%d'}}
    result = check_timestamp_fields(df, imputed_columns)
    expected = {'timestamp_fields_found': ['date1', 'date2'], 'timestamp_issues_percentage': 25.0}
    assert result == expected

def test_check_timestamp_fields_no_imputed_columns():
    data = {'date': ['2022-01-01', '2022-01-02', '2022-01-03', '2022-01-04']}
    df = pd.DataFrame(data)
    result = check_timestamp_fields(df)
    expected = {'timestamp_fields_found': 'None', 'timestamp_issues_percentage': 'None'}
    assert result == expected