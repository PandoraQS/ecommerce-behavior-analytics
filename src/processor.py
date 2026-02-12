import pandas as pd
import sqlite3
import json
import os
from pydantic import BaseModel, Field, ValidationError
from typing import Optional, List

class LogSchema(BaseModel):
    user_id: str
    timestamp: str
    action: str
    category: str
    amount: float = Field(ge=0)
    session_id: str

def run_pipeline(input_path, db_path):
    print(f"--- Starting data processing from: {input_path} ---")
    
    if not os.path.exists(input_path):
        print("Error: raw_logs.json not found.")
        return

    with open(input_path, 'r') as f:
        raw_data = json.load(f)
    
    print("Validating data schema...")
    validated_data = []
    errors_count = 0

    for entry in raw_data:
        try:
            # If entry is missing a field or amount is negative, this fails
            obj = LogSchema(**entry)
            validated_data.append(obj.model_dump())
        except ValidationError as e:
            errors_count += 1
            continue

    print(f"Validation complete. Passed: {len(validated_data)}, Failed: {errors_count}")

    df = pd.DataFrame(validated_data)
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='ISO8601')
    df = df.sort_values(by=['user_id', 'timestamp'])

    # --- BEHAVIORAL ANALYTICS: Detecting Impulsivity ---
    # Calculate the time difference between consecutive actions for each user
    df['time_diff'] = df.groupby('user_id')['timestamp'].diff().dt.total_seconds()

    # Define "Impulsive/High-Frequency" behavior as actions occurring in less than 2 seconds
    df['is_high_frequency'] = df['time_diff'].apply(lambda x: 1 if x is not None and x < 2 else 0)

    conn = sqlite3.connect(db_path)
    df.to_sql('user_behavior', conn, if_exists='replace', index=False)
    conn.close()
    print(f"--- Pipeline complete! Database created at: {db_path} ---")

if __name__ == "__main__":
    run_pipeline('data/raw_logs.json', 'data/ecommerce_analytics.db')