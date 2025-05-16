# Data Readiness Framework
## Telangana AI Sandbox


| Metric                 | Weight | How Score is Computed                          |
|------------------------|--------|------------------------------------------------|
| column_missing         | 15     | Decreases with % missing columns               |
| row_missing            | 10     | Decreases with % rows with >50% missing        |
| exact_row_duplicates   | 10     | Decreases with % duplicate rows                |
| coverage_check         | 10     | Decreases with % missing region coverage       |
| numeric_variance       | 5      | Decreases with % low-variance numeric columns  |
| categorical_variation  | 5      | Decreases with % dominated categorical columns |
| file_format_check      | 5      | Full points if valid, else 0                   |
| uniform_encoding       | 10     | Decreases with % date/encoding issues          |
| label_presence         | 10     | Proportional to % non-null label presence      |
| timestamp_fields_found | 10     | Decreases with % timestamp issues              |
| documentation_presence | 10     | Full points if present, else 0                 |
