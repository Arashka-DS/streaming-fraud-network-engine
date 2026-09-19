CREATE TABLE IF NOT EXISTS transaction_audit (
    tx_id VARCHAR(50) PRIMARY KEY,
    sender VARCHAR(50),
    receiver VARCHAR(50),
    amount NUMERIC,
    is_anomaly BOOLEAN,
    is_mule BOOLEAN,
    louvain_community_id INTEGER,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
