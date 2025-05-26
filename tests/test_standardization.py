import pytest
import os
import pandas as pd
from metrics.standardization import check_file_format, check_date_and_timestamp_format 

@pytest.fixture(autouse=True)
def temp_dir():
    temp_dir = 'temp_dir'
    if not os.path.exists(temp_dir):
        os.mkdir(temp_dir)
    yield temp_dir
    for file in os.listdir(temp_dir):
        file_path = os.path.join(temp_dir, file)
        if os.path.isfile(file_path):
            os.remove(file_path)

def test_valid_file_format(temp_dir):
    # Create a valid file in the temporary directory
    with open(os.path.join(temp_dir, 'test.csv'), 'w') as f:
        f.write('test')
    assert check_file_format(temp_dir) == {"file_format": "valid"}

def test_invalid_file_format(temp_dir):
    # Create an invalid file in the temporary directory
    with open(os.path.join(temp_dir, 'test.txt'), 'w') as f:
        f.write('test')
    assert check_file_format(temp_dir) == {"file_format": "invalid"}

def test_empty_directory(temp_dir):
    # The temporary directory is already empty
    assert check_file_format(temp_dir) == {"file_format": "invalid"}

    # Define df and imputed_columns for the test
    df = pd.DataFrame({'date': ['2022-01-01', '2022-01-02']})
    imputed_columns = {'date': {'column': ['date'], 'format': '%Y-%m-%d'}}
    result = check_date_and_timestamp_format(df, imputed_columns)
    expected = {"date_column": ['date'], "timestamp_column": [], "number_of_date_columns": 1, "number_of_timestamp_columns": 0.0, "datetime_issues_percentage": 0.0}
    assert result == expected

def test_check_date_and_timestamp_format_valid_date():
    df = pd.DataFrame({'date': ['2022-01-01', '2022-01-02']})
    imputed_columns = {'date': {'column': ['date'], 'format': '%Y-%m-%d'}}
    result = check_date_and_timestamp_format(df, imputed_columns)
    expected = {"date_column": ['date'], "timestamp_column": [], "number_of_date_columns": 1, "number_of_timestamp_columns": 0.0, "datetime_issues_percentage": 0.0}
    assert result == expected

def test_check_date_and_timestamp_format_invalid_date():
    df = pd.DataFrame({'date': ['2022-01-01', ' invalid']})
    imputed_columns = {'date': {'column': ['date'], 'format': '%Y-%m-%d'}}
    result = check_date_and_timestamp_format(df, imputed_columns)
    expected = {"date_column": ['date'], "timestamp_column": [], "number_of_date_columns": 1, "number_of_timestamp_columns": 0.0, "datetime_issues_percentage": 50.0}
    assert result == expected

def test_check_date_and_timestamp_format_missing_date_column():
    df = pd.DataFrame({'other_column': [1, 2]})
    imputed_columns = {'date': {'column': ['date'], 'format': '%Y-%m-%d'}}
    result = check_date_and_timestamp_format(df, imputed_columns)
    expected = {"date_column": [], "timestamp_column": [], "number_of_date_columns": 0, "number_of_timestamp_columns": 0.0, "datetime_issues_percentage": 0.0}
    assert result == expected

def test_check_date_and_timestamp_format_empty_df():
    df = pd.DataFrame()
    imputed_columns = {}
    result = check_date_and_timestamp_format(df, imputed_columns)
    expected = {"date_column": "None", "timestamp_column": "None", "number_of_date_columns": 0, "number_of_timestamp_columns": 0.0, "datetime_issues_percentage": 'None'}
    assert result == expected
