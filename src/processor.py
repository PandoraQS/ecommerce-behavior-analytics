import pandas as pd
import sqlite3
import json
import os
from pathlib import Path
from pydantic import BaseModel, Field, ValidationError

class LogSchema(BaseModel):
    user_id: str
    timestamp: str
    action: str
    category: str
    amount: float = Field(ge=0)
    session_id: str

def run_pipeline(input_path_str, db_path_str):
    BASE_DIR = Path(__file__).resolve().parent.parent
    safe_input_path = (BASE_DIR / input_path_str).resolve()
    safe_db_path = (BASE_DIR / db_path_str).resolve()

    if not str(safe_input_path).startswith(str(BASE_DIR)):
        raise ValueError("Unsafe path detected")

    print(f"--- Starting data processing from: {safe_input_path} ---")
    if not safe_input_path.exists(): return

    with open(safe_input_path, 'r') as f:
        raw_data = json.load(f)
    
    print("Validating data schema...")
    validated_data = []
    for entry in raw_data:
        try:
            obj = LogSchema(**entry)
            validated_data.append(obj.model_dump())
        except ValidationError: continue

    df = pd.DataFrame(validated_data)
    
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='ISO8601')
    df = df.sort_values(by=['user_id', 'timestamp'])

    # --- Behavioral analytics: Latency ---
    df['time_diff'] = df.groupby('user_id')['timestamp'].diff().dt.total_seconds()
    df['rolling_avg_latency'] = (
        df.groupby('user_id')['time_diff']
        .rolling(window=5, min_periods=1)
        .mean()
        .reset_index(level=0, drop=True)
    ).round(2)

    # --- Volatility (Standard Deviation) ---
    df['amount_volatility'] = (
        df.groupby('user_id')['amount']
        .rolling(window=5, min_periods=1)
        .std()
        .reset_index(level=0, drop=True)
    ).fillna(0).round(2)

    # --- Session Fatigue ---
    df['session_duration_min'] = df.groupby(['user_id', 'session_id'])['timestamp'].transform(
        lambda x: (x.max() - x.min()).total_seconds() / 60
    ).round(2)

    # --- Time-per-day: hour for analysis ---
    df['hour'] = df['timestamp'].dt.hour

    impulsive_actions = ['page_refresh', 'add_to_cart']
    df['is_high_risk'] = df.apply(
        lambda x: 1 if (
            (pd.notnull(x['rolling_avg_latency']) and x['rolling_avg_latency'] < 3.0 and x['action'] in impulsive_actions) or
            (x['amount_volatility'] > 150.0) or 
            (x['session_duration_min'] > 30.0)
        ) else 0, axis=1)

    conn = sqlite3.connect(safe_db_path)
    df.to_sql('user_behavior', conn, if_exists='replace', index=False)
    conn.close()
    print(f"--- Pipeline complete! ---")

if __name__ == "__main__":
    run_pipeline('data/raw_logs.json', 'data/ecommerce_analytics.db')