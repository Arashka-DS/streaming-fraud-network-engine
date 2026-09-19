import os
import json
import time
import random
import uuid
from datetime import datetime
from kafka import KafkaProducer

# Dynamic routing for Docker vs Host execution
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:19092")
TOPIC = "transactions"

print(f"Initializing Redpanda/Kafka Producer targeting broker: {KAFKA_BROKER}")

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_normal_transaction():
    """Generates standard, benign transaction behavior."""
    return {
        "transaction_id": f"TXN-{uuid.uuid4().hex[:8]}",
        "source_account": f"ACC-{random.randint(1000, 5000)}",
        "destination_account": f"ACC-{random.randint(1000, 5000)}",
        "amount": round(random.uniform(10_000, 500_000), 2),
        "timestamp": datetime.utcnow().isoformat()
    }

def generate_mule_ring():
    """Simulates a pass-through mule account (A -> Mule -> B)."""
    mule_account = f"MULE-{random.randint(9000, 9050)}"
    source = f"ACC-{random.randint(1000, 5000)}"
    destination = f"ACC-{random.randint(1000, 5000)}"
    base_amount = round(random.uniform(2_000_000, 5_000_000), 2)
    
    return [
        {
            "transaction_id": f"TXN-{uuid.uuid4().hex[:8]}",
            "source_account": source,
            "destination_account": mule_account,
            "amount": base_amount,
            "timestamp": datetime.utcnow().isoformat()
        },
        {
            "transaction_id": f"TXN-{uuid.uuid4().hex[:8]}",
            "source_account": mule_account,
            "destination_account": destination,
            "amount": base_amount - random.uniform(1000, 5000), # Minus small fee
            "timestamp": datetime.utcnow().isoformat()
        }
    ]

def generate_benford_anomaly():
    """Spams transactions starting with 8 or 9 to intentionally break Benford's Law."""
    return {
        "transaction_id": f"TXN-{uuid.uuid4().hex[:8]}",
        "source_account": f"BOT-{random.randint(9900, 9999)}",
        "destination_account": f"ACC-{random.randint(1000, 5000)}",
        "amount": round(random.uniform(8_000_000, 9_999_999), 2),
        "timestamp": datetime.utcnow().isoformat()
    }

if __name__ == "__main__":
    print(f"Starting transaction stream to topic '{TOPIC}'...")
    try:
        while True:
            scenario = random.random()
            
            if scenario < 0.70:
                # 70% Normal Traffic
                tx = generate_normal_transaction()
                producer.send(TOPIC, tx)
                print(f"[NORMAL] Sent: {tx['transaction_id']} | {tx['amount']} IRR")
                time.sleep(random.uniform(0.1, 0.5))
                
            elif scenario < 0.85:
                # 15% Mule Ring Activity
                mule_txs = generate_mule_ring()
                for tx in mule_txs:
                    producer.send(TOPIC, tx)
                    print(f"[MULE RING] Sent: {tx['transaction_id']} via {tx['source_account']}")
                    time.sleep(0.05)
                
            elif scenario < 0.95:
                # 10% Benford's Law Bot Injection
                tx = generate_benford_anomaly()
                producer.send(TOPIC, tx)
                print(f"[SYNTHETIC BOT] Sent: {tx['transaction_id']} | {tx['amount']} IRR")
                time.sleep(0.1)
                
            else:
                # 5% Velocity Burst (Card Testing Simulation)
                bad_actor = f"HACKER-{random.randint(1, 10)}"
                for _ in range(6): # Triggers the Redis >5/min threshold
                    tx = generate_normal_transaction()
                    tx["source_account"] = bad_actor
                    producer.send(TOPIC, tx)
                    print(f"[VELOCITY BURST] Sent: {tx['transaction_id']} from {bad_actor}")
                time.sleep(1)

    except KeyboardInterrupt:
        print("\nStopping producer.")
    finally:
        producer.flush()
        producer.close()
