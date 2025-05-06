# tests/test_quality.py
import pandas as pd
from metrics.quality import check_column_missing, check_row_missing

def test_check_column_missing():
    # Create a sample DataFrame
    data = {'A': [1, 2, None, 4], 'B': [None, None, None, None], 'C': [1, 2, 3, 4]}
    df = pd.DataFrame(data)

    # Test with default threshold
    result = check_column_missing(df)
    expected = {
        "column_missing": {'A': 25.0, 'B': 100.0},
        "column_missing_count": 2,
        "column_missing_percentage": 66.67
    }
    assert result == expected

    # Test with custom threshold
    result = check_column_missing(df, threshold=0.5)
    expected = {
        "column_missing": {'B': 100.0},
        "column_missing_count": 1,
        "column_missing_percentage": 33.33
    }
    assert result == expected

    # Test with no missing values
    data = {'A': [1, 2, 3, 4], 'B': [1, 2, 3, 4], 'C': [1, 2, 3, 4]}
    df = pd.DataFrame(data)
    result = check_column_missing(df)
    expected = {
        "column_missing": {},
        "column_missing_count": 0,
        "column_missing_percentage": 0.0
    }
    assert result == expected

def test_check_row_missing():
    # Create a sample DataFrame
    data = {'A': [1, None, 3, 4], 'B': [None, 2, None, 4], 'C': [1, 2, 3, 4]}
    df = pd.DataFrame(data)

    # Test with default threshold
    result = check_row_missing(df)
    expected = {
        "row_missing": {0: 50.0, 1: 50.0, 2: 33.33},
        "row_missing_count": 3,
        "row_missing_percentage": 75.0
    }
    assert result == expected

    # Test with custom threshold
    result = check_row_missing(df, threshold=0.7)
    expected = {
        "row_missing": {0: 50.0, 1: 50.0},
        "row_missing_count": 2,
        "row_missing_percentage": 50.0
    }
    assert result == expected

    # Test with no missing values
    data = {'A': [1, 2, 3, 4], 'B': [1, 2, 3, 4], 'C': [1, 2, 3, 4]}
    df = pd.DataFrame(data)
    result = check_row_missing(df)
    expected = {
        "row_missing": {},
        "row_missing_count": 0,
        "row_missing_percentage": 0.0
    }
    assert result == expected

def test_check_column_missing_empty_df():
    df = pd.DataFrame()
    result = check_column_missing(df)
    expected = {
        "column_missing": {},
        "column_missing_count": 0,
        "column_missing_percentage": 0.0
    }
    assert result == expected

def test_check_row_missing_empty_df():
    df = pd.DataFrame()
    result = check_row_missing(df)
    expected = {
        "row_missing": {},
        "row_missing_count": 0,
        "row_missing_percentage": 0.0
    }
    assert result == expected