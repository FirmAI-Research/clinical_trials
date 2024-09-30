"""
This script connects to DuckDB, fetches conditions/diseases from the clinicaltrials_mart table,
and uses GPT-3.5 to standardize the condition names. The output is saved back to DuckDB.
Make sure the OpenAI API key is set in your environment.
"""

import os
import duckdb
from openai import OpenAI

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
con = duckdb.connect(database='clinical_trials.duckdb')

# Query the conditions (diseases) from the clinicaltrials_mart
query = "SELECT nct_id, condition FROM clinicaltrials_mart WHERE condition IS NOT NULL"
df = con.execute(query).fetchdf()

def standardize_conditions(text):
    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "You are a helpful assistant that standardizes medical conditions."},
            {"role": "user", "content": f"Standardize the following condition: {text}"}
        ]
    )
    return response.choices[0].message.content

# Apply standardization to the conditions column
df['standardized_condition'] = df['condition'].apply(lambda x: standardize_conditions(x) if x else None)

# Save the standardized conditions back to DuckDB
df.to_parquet('standardized_conditions.parquet')
con.execute("CREATE TABLE IF NOT EXISTS standardized_clinicaltrials AS SELECT * FROM read_parquet('standardized_conditions.parquet')")

print("Condition standardization completed.")

