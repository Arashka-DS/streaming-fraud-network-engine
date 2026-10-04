from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import redis
import json
import time
import os
import psycopg2
import threading
from kafka import KafkaConsumer
from src.fraud_engine import FraudGraphEngine

app = FastAPI(title="Network Fraud Streaming API")
redis_client = redis.Redis(host=os.getenv("REDIS_HOST", "localhost"), port=6379, decode_responses=True)
fraud_engine = FraudGraphEngine()

# Global cache for heavy graph computations
cached_communities = {}

def update_graph_metrics():
    """Background task to compute heavy graph algorithms without blocking the stream."""
    global cached_communities
    while True:
        time.sleep(60) # Run every 60 seconds
        if len(fraud_engine.G) > 0:
            cached_communities = fraud_engine.compute_louvain_communities()
            print("[System] Louvain communities updated.")

def log_to_postgres(tx_id, sender, receiver, amount, velocity_score, is_anomaly, is_mule, community_id):
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            database=os.getenv("DB_NAME", "fraud_warehouse"),
            user=os.getenv("DB_USER", "fraud_admin"), 
            password=os.getenv("DB_PASSWORD", "fraud_password")
        )
        cursor = conn.cursor()
        # Add velocity_score to the INSERT statement
        cursor.execute("""
            INSERT INTO transaction_audit (tx_id, sender, receiver, amount, velocity_score, is_anomaly, is_mule, louvain_community_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (tx_id, sender, receiver, amount, velocity_score, is_anomaly, is_mule, community_id))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"DB Error: {e}", flush=True)

def consume_transactions():
    consumer = KafkaConsumer(
        'transactions',
        bootstrap_servers=os.getenv("KAFKA_BROKER", "localhost:19092"),
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest'
    )
    
def consume_transactions():
    consumer = KafkaConsumer(
        'transactions',
        bootstrap_servers=os.getenv("KAFKA_BROKER", "localhost:19092"),
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest'
    )
    
   for message in consumer:
        tx = message.value
        sender = tx['source_account']
        receiver = tx['destination_account']
        amount = tx['amount']
        tx_id = tx['transaction_id']
        
        # Fast Redis Velocity Check
        redis_key = f"velocity:{sender}"
        timestamp = time.time()
        redis_client.zadd(redis_key, {f"{tx_id}|{amount}": timestamp})
        redis_client.zremrangebyscore(redis_key, 0, timestamp - 3600)
        velocity_count = redis_client.zcard(redis_key)

        # Fast ML & Heuristics Check
        flags = fraud_engine.process_transaction(sender, receiver, amount, velocity_count)
        
        # Cast NumPy booleans to native Python booleans for psycopg2
        is_anomaly = bool(flags['is_anomaly'])
        is_mule = bool(flags['is_mule'])
        
        # O(1) Lookup against cached graph communities
        community_id = cached_communities.get(sender, -1)

        # Asynchronous DB Write
        threading.Thread(
            target=log_to_postgres, 
            args=(tx_id, sender, receiver, amount, velocity_count, is_anomaly, is_mule, community_id)
        ).start()

@app.on_event("startup")
async def startup_event():
    threading.Thread(target=consume_transactions, daemon=True).start()
    threading.Thread(target=update_graph_metrics, daemon=True).start()

@app.get("/metrics/benford")
def get_audit_metrics():
    return fraud_engine.check_benfords_law()
