import pytest
import os
import pandas as pd
from metrics.standardization import check_file_format, check_date_format  # replace 'your_module' with the actual module name

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

def test_valid_date_format():
    # Create a DataFrame with a valid date column
    df = pd.DataFrame({'date': ['2022-01-01', '2022-01-02']})
    imputed_columns = {'date': {'column': ['date'], 'format': '%Y-%m-%d'}}
    result = check_date_format(df, imputed_columns)
    assert result['date_column'] == ['date']
    assert result['date_issues_percentage'] == {'date': 0.0}

def test_invalid_date_format():
    # Create a DataFrame with an invalid date column
    df = pd.DataFrame({'date': ['2022-01-01', ' invalid']})
    imputed_columns = {'date': {'column': ['date'], 'format': '%Y-%m-%d'}}
    result = check_date_format(df, imputed_columns)
    assert result['date_column'] == ['date']
    assert result['date_issues_percentage'] == {'date': 50.0}

def test_missing_date_column():
    # Create a DataFrame without the specified date column
    df = pd.DataFrame({'other_column': [1, 2]})
    imputed_columns = {'date': {'column': ['date'], 'format': '%Y-%m-%d'}}
    result = check_date_format(df, imputed_columns)
    assert result['date_column'] == []
    assert result['date_issues_percentage'] == {}

def test_missing_date_format():
    # Create a DataFrame with a date column but without a specified format
    df = pd.DataFrame({'date': ['2022-01-01', '2022-01-02']})
    imputed_columns = {'date': {'column': ['date']}}
    result = check_date_format(df, imputed_columns)
    assert result == {'date_column': "No columns or format specified", 'date_issues_percentage': 'None'}