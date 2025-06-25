# tests/test_model_ingestible.py

import pandas as pd
from structured_metrics.model_ingestible import check_label_presence

def test_label_column_present():
    # Create a sample dataframe with a label column
    df = pd.DataFrame({'label': [1, 2, 3, 4, 5], 'feature1': [10, 20, 30, 40, 50]})
    imputed_columns = {"label": "label"}
    
    # Call the function and assert the result
    result = check_label_presence(df, imputed_columns)
    assert result == {'label_presence_count': '100.00', 'label_column': 'label'}

def test_label_column_missing():
    # Create a sample dataframe without a label column
    df = pd.DataFrame({'feature1': [10, 20, 30, 40, 50], 'feature2': [100, 200, 300, 400, 500]})
    imputed_columns = {}
    
    # Call the function and assert the result
    result = check_label_presence(df, imputed_columns)
    assert result == {'label_presence_count': 'None', 'label_column': 'Label column not found'}

def test_label_column_with_null_values():
    # Create a sample dataframe with a label column containing null values
    df = pd.DataFrame({'label': [1, 2, None, 4, 5], 'feature1': [10, 20, 30, 40, 50]})
    imputed_columns = {'label': 'label'}
    
    # Call the function and assert the result
    result = check_label_presence(df, imputed_columns)
    assert result == {'label_presence_count': '80.00', 'label_column': 'label'}

def test_imputed_columns_is_none():
    # Create a sample dataframe with a label column
    df = pd.DataFrame({'label': [1, 2, 3, 4, 5], 'feature1': [10, 20, 30, 40, 50]})
    
    # Call the function with imputed_columns as None and assert the result
    result = check_label_presence(df)
    assert result == {'label_presence_count': 'None', 'label_column': 'Label column not found'}