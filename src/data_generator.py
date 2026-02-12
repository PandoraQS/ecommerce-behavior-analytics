import pandas as pd
import random
import json
from faker import Faker
from datetime import datetime, timedelta
import os

fake = Faker()

def generate_mock_data(num_users=100, num_events=2000):
    events = []
    actions = ['view_product', 'add_to_cart', 'checkout_start', 'purchase', 'page_refresh']
    categories = ['electronics', 'clothing', 'home', 'toys', 'beauty']
    user_ids = [f"user_{str(i).zfill(3)}" for i in range(num_users)]
    impulsive_users_list = random.sample(user_ids, int(num_users * 0.15))
    
    for _ in range(num_events):
        user = random.choice(user_ids)
        action = random.choice(actions)
        timestamp = fake.date_time_between(start_date='-1d', end_date='now') if random.random() > 0.05 else None
        
        events.append({
            "user_id": user,
            "timestamp": timestamp.isoformat() if timestamp else None,
            "action": action,
            "category": random.choice(categories),
            "amount": round(random.uniform(10.0, 500.0), 2) if action == 'purchase' else 0,
            "session_id": f"sess_{random.randint(100, 999)}"
        })

    for user in impulsive_users_list:
        base_time = datetime.now() - timedelta(hours=random.randint(1, 24))
        burst_size = random.randint(12, 30)
        
        for i in range(burst_size):
            interval = random.uniform(0.5, 2.5) 
            base_time += timedelta(seconds=interval)
            
            events.append({
                "user_id": user,
                "timestamp": base_time.isoformat(),
                "action": random.choice(['page_refresh', 'add_to_cart']),
                "category": random.choice(categories),
                "amount": 0,
                "session_id": f"sess_burst_{user}"
            })

    return events

if __name__ == "__main__":
    print("Data generation in progress...")
    data = generate_mock_data()
    if not os.path.exists('data'): os.makedirs('data')
    with open('data/raw_logs.json', 'w') as f:
        json.dump(data, f, indent=4)
    print(f"data/raw_logs.json created with random risk distribution. Events: {len(data)}")