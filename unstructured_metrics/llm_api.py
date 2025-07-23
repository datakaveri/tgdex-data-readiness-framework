import openai
import json
import pandas as pd

def infer_column_roles_openai(metadata, api_key):
    # Initialize client
    client = openai.OpenAI(api_key=api_key)

    system_prompt = (
"You are a data analyst helping assess unstructured datasets for data readiness.\n"
"Given:\n"
"A list of file metadata entries (including fields like filename, folder path, title, description, tags, or keywords).\n"
"Your tasks are:\n\n"

"1. Detect and extract **region or geographic indicators** from the metadata. Look for references to Indian administrative divisions such as 'district', 'mandal', 'taluk', 'tehsil', 'village', 'ward', 'zone', or 'locality'.\n"
"These may be written informally, abbreviated, or embedded in longer names (e.g., 'Hyd', 'Secbad', 'ADBD', or full names like 'Nellore', 'Tirumalagiri').\n"
"Return each region mention along with the metadata field it was found in (e.g., title, tags, filename).\n\n"

"2. Identify any **timestamp-related information** in the metadata. This includes file creation times, modification times, version numbers, or any date-time formats present (e.g., '2023-08-15', '15/08/2023 14:32').\n"
"For each detected value, return the text, the metadata field, and the most likely Python datetime format (e.g., '%Y-%m-%d %H:%M:%S').\n\n"

"Return your output as a JSON object with the following structure:\n"
"{\n"
"  \"regions\": [\"list_of_metadata_attributes\"],\n"
"  \"timestamps\": [\"list_of_timestamp_attributes\"]\n"
"}\n\n"

"If no values are found in a category, return an empty list for that key."
)


    user_prompt = (
        f"Here is the metadata:\n{json.dumps(metadata, indent=2)}"
    )

    response = client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        temperature=0,
        top_p=1
    )

    message_content = response.choices[0].message.content

    try:
        return json.loads(message_content)
    except json.JSONDecodeError:
        return {
            "error": "Could not parse OpenAI response",
            "raw_response": message_content
        }