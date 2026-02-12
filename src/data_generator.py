import pandas as pd
import random
import json
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

def generate_mock_data(num_users=10, num_events=200):
    events = []
    actions = ['view_product', 'add_to_cart', 'checkout_start', 'purchase', 'page_refresh']
    categories = ['electronics', 'clothing', 'home', 'toys', 'beauty']
    
    user_ids = [f"user_{i}" for i in range(num_users)]
    
    for _ in range(num_events):
        user = random.choice(user_ids)
        action = random.choice(actions)
        
        timestamp = fake.date_time_between(start_date='-1d', end_date='now') if random.random() > 0.05 else None
        
        event = {
            "user_id": user,
            "timestamp": timestamp.isoformat() if timestamp else None,
            "action": action,
            "category": random.choice(categories),
            "amount": round(random.uniform(10.0, 500.0), 2) if action == 'purchase' else 0,
            "session_id": f"sess_{random.randint(100, 150)}"
        }
        events.append(event)

    impulsive_user = "user_999"
    base_time = datetime.now()
    for i in range(15):
        events.append({
            "user_id": impulsive_user,
            "timestamp": (base_time + timedelta(seconds=i)).isoformat(),
            "action": "page_refresh",
            "category": "electronics",
            "amount": 0,
            "session_id": "sess_999"
        })

    return events

if __name__ == "__main__":
    print("Data generation in progress...")
    data = generate_mock_data()
    
    with open('data/raw_logs.json', 'w') as f:
        json.dump(data, f, indent=4)
    
    print(f"data/raw_logs.json file created with {len(data)} events.")