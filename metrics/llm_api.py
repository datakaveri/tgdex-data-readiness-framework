import openai
import json
import pandas as pd

def infer_column_roles_openai(df, api_key):
    # Initialize client
    client = openai.OpenAI(api_key=api_key)

    column_names = df.columns.tolist()
    first_rows = df.head(5).to_dict(orient="records")

    system_prompt = (
        "You are a data analyst helping identify key columns in a tabular dataset. "
        "Given the column names and a few data rows, identify:\n\n"
        "1. A column most likely representing a **region** (e.g., state, district, area, or zone). These may also include abbreviations or words that are relevant to the Indian context (e.g. taluk, tehsil, village, mandal, etc.) \n"
        "2. A column most likely representing a **date**. Also return the most likely Python datetime format string (e.g., %Y-%m-%d or %d/%m/%Y) that can be used with datetime.strptime() or pandas.to_datetime() to correctly parse the values. "
        "(e.g., YYYY-MM-DD, DD/MM/YYYY, etc.).\n"
        "3. A column most likely representing a **timestamp**(e.g. created_at, updated_at). Also return the most likely Python datetime format string (e.g., %Y-%m-%d %H:%M:%S) that can be used to parse them with datetime.strptime(). If there are any variations, include those exactly in the format string.\n"
        "4. A column that is most likely to be the prediction target (label) in a machine learning task. This column might represent an outcome such as employment status, eligibility, literacy, etc. It could contain binary, categorical, or numeric values depending on the problem.\n\n"
        "There may be multiple columns that match all of these categories. If so, return all of them as a list.\n\n"
        "Return your answer as a JSON object with this structure:\n"
        "{\n"
        '  "region": "column_name_or_null",\n'
        '  "date": {\n'
        '    "column": "column_name_or_null",\n'
        '    "format": "date_format_or_null"\n'
        "  },\n"
        '  "timestamp": {\n'
        '    "column": "column_name_or_null",\n'
        '    "format": "date_format_or_null"\n'
        "  },\n"
        '  "label": "column_name_or_null"\n'
        "}\n\n"
        "If no match is found for a category, use null."
    )

    user_prompt = (
        f"Here are the column names:\n{column_names}\n\n"
        f"And here are the first 5 rows of data:\n{json.dumps(first_rows, indent=2)}"
    )

    response = client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        temperature=0.2
    )

    message_content = response.choices[0].message.content

    try:
        return json.loads(message_content)
    except json.JSONDecodeError:
        return {
            "error": "Could not parse OpenAI response",
            "raw_response": message_content
        }