import pandas as pd
from metrics.quality import check_column_missing, check_row_missing, check_row_duplicates
# TODO: Pytest fixtures
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
        "row_missing_count": 3,
        "row_missing_percentage": 75.0
    }
    assert result == expected

    data = {'A': [1, None, 3, 4], 'B': [None, None, None, 4], 'C': [1, 2, 3, 4]}
    df = pd.DataFrame(data)

    # Test with custom threshold
    result = check_row_missing(df, threshold=0.6)
    expected = {
        "row_missing_count": 1,
        "row_missing_percentage": 25.0
    }
    assert result == expected

    # Test with no missing values
    data = {'A': [1, 2, 3, 4], 'B': [1, 2, 3, 4], 'C': [1, 2, 3, 4]}
    df = pd.DataFrame(data)
    result = check_row_missing(df)
    expected = {
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
        "row_missing_count": 0,
        "row_missing_percentage": 0.0
    }
    assert result == expected

def test_check_column_missing_all_null():
    data = {'A': [None, None, None], 'B': [None, None, None]}
    df = pd.DataFrame(data)
    result = check_column_missing(df)
    expected = {
        "column_missing": {'A': 100.0, 'B': 100.0},
        "column_missing_count": 2,
        "column_missing_percentage": 100.0
    }
    assert result == expected
    # Test with empty column

def test_check_row_duplicates():
    data = {'A': [1, 2, 3, 4, 1], 'B': [1, 2, 3, 4, 1]}
    df = pd.DataFrame(data)
    result = check_row_duplicates(df)
    expected = {
        "exact_row_duplicates": 1,
        "exact_row_duplicates_percentage": 20.0
    }
    assert result == expected
