import json
import time
import uuid
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:19092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

accounts = [f"ACC_{i}" for i in range(1, 50)]
mule_hub = "ACC_MULE_99" # Deliberately engineered mule node

print("Streaming synthetic transactions to Redpanda...")
while True:
    # 90% Normal Traffic
    sender = random.choice(accounts)
    receiver = random.choice(accounts)
    amount = round(random.uniform(10.0, 500.0), 2)
    
    # 10% Mule Ring Traffic Injection (Cycling through hub)
    if random.random() > 0.9:
        if random.random() > 0.5:
            sender = random.choice(accounts)
            receiver = mule_hub
            amount = round(random.uniform(2000.0, 5000.0), 2)
        else:
            sender = mule_hub
            receiver = random.choice(accounts)
            amount = round(random.uniform(2000.0, 5000.0), 2)

    tx = {
        "tx_id": str(uuid.uuid4()),
        "sender": sender,
        "receiver": receiver,
        "amount": amount
    }
    
    producer.send('transactions', tx)
    time.sleep(0.5) # Simulate real-time streaming
