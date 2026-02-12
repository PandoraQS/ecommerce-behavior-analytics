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

def get_safe_path(base_dir: Path, target_path_str: str) -> Path:
    target_path = (base_dir / target_path_str).resolve()
    if not str(target_path).startswith(str(base_dir.resolve())):
        raise PermissionError(f"Access denied: {target_path_str} is outside base directory.")
    return target_path

def run_pipeline(input_rel_path, db_rel_path):
    BASE_DIR = Path(__file__).resolve().parent.parent
    
    try:
        safe_input_path = get_safe_path(BASE_DIR, input_rel_path)
        safe_db_path = get_safe_path(BASE_DIR, db_rel_path)

        print(f"--- Processing data from: {safe_input_path.name} ---")
        
        if not safe_input_path.exists():
            print(f"Error: File {safe_input_path} not found.")
            return

        with open(safe_input_path, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
        
        validated_data = []
        for entry in raw_data:
            try:
                obj = LogSchema(**entry)
                validated_data.append(obj.model_dump())
            except ValidationError:
                continue

        if not validated_data:
            print("No valid data to process.")
            return

        df = pd.DataFrame(validated_data)
        df['timestamp'] = pd.to_datetime(df['timestamp'], format='ISO8601')
        df = df.sort_values(by=['user_id', 'timestamp'])

        df['time_diff'] = df.groupby('user_id')['timestamp'].diff().dt.total_seconds()
        df['rolling_avg_latency'] = (
            df.groupby('user_id')['time_diff']
            .rolling(window=5, min_periods=1)
            .mean()
            .reset_index(level=0, drop=True)
        ).round(2)

        df['amount_volatility'] = (
            df.groupby('user_id')['amount']
            .rolling(window=5, min_periods=1)
            .std()
            .reset_index(level=0, drop=True)
        ).fillna(0).round(2)

        df['session_duration_min'] = df.groupby(['user_id', 'session_id'])['timestamp'].transform(
            lambda x: (x.max() - x.min()).total_seconds() / 60
        ).round(2)

        df['hour'] = df['timestamp'].dt.hour

        impulsive_actions = ['page_refresh', 'add_to_cart']
        df['is_high_risk'] = df.apply(
            lambda x: 1 if (
                (pd.notnull(x['rolling_avg_latency']) and x['rolling_avg_latency'] < 3.0 and x['action'] in impulsive_actions) or
                (x['amount_volatility'] > 150.0) or 
                (x['session_duration_min'] > 30.0)
            ) else 0, axis=1)

        with sqlite3.connect(safe_db_path) as conn:
            df.to_sql('user_behavior', conn, if_exists='replace', index=False)
            
        print(f"--- Pipeline complete! Database updated at: {safe_db_path.name} ---")

    except (PermissionError, ValueError) as e:
        print(f"Security Alert: {e}")
    except Exception as e:
        print(f"Pipeline Error: {e}")

if __name__ == "__main__":
    run_pipeline('data/raw_logs.json', 'data/ecommerce_analytics.db')