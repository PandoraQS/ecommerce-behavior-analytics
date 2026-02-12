import pandas as pd
import sqlite3
import json
import os

def run_pipeline(input_path, db_path):
    print(f"--- Starting data processing from: {input_path} ---")
    
    if not os.path.exists(input_path):
        print("Error: raw_logs.json not found. Please run data_generator.py first.")
        return

    with open(input_path, 'r') as f:
        raw_data = json.load(f)
    
    df = pd.DataFrame(raw_data)

    print("Cleaning and validating data...")
    
    initial_len = len(df)
    # Removing events without a timestamp
    df = df.dropna(subset=['timestamp']) 
    print(f"Removed {initial_len - len(df)} rows with missing timestamps.")

    df['timestamp'] = pd.to_datetime(df['timestamp'], format='ISO8601')    
    df = df.sort_values(by=['user_id', 'timestamp'])

    # --- BEHAVIORAL ANALYTICS: Detecting Impulsivity ---
    # Calculate the time difference between consecutive actions for each user
    df['time_diff'] = df.groupby('user_id')['timestamp'].diff().dt.total_seconds()

    # Define "Impulsive/High-Frequency" behavior as actions occurring in less than 2 seconds
    df['is_high_frequency'] = df['time_diff'].apply(lambda x: 1 if x is not None and x < 2 else 0)

    print(f"Loading {len(df)} records into the SQL database...")
    conn = sqlite3.connect(db_path)
    
    df.to_sql('user_behavior', conn, if_exists='replace', index=False)
    
    conn.close()
    print(f"--- Pipeline complete! Database created at: {db_path} ---")

if __name__ == "__main__":
    run_pipeline('data/raw_logs.json', 'data/ecommerce_analytics.db')