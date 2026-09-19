-- Card 1: Real-Time Fraud Velocity (Line Chart - Neon Red)

SELECT 
    date_trunc('minute', timestamp) AS tx_minute, 
    COUNT(*) AS total_flagged
FROM transaction_audit 
WHERE is_mule = TRUE OR is_anomaly = TRUE 
GROUP BY 1 
ORDER BY tx_minute DESC
LIMIT 60;

-- Card 2: Louvain Fraud Ring Exposure (Bubble/Scatter Chart - Nodes by Fiat Exposure)

SELECT 
    louvain_community_id, 
    COUNT(DISTINCT sender) as nodes_in_ring, 
    SUM(amount) as total_fiat_exposure
FROM transaction_audit 
WHERE louvain_community_id != -1
GROUP BY 1 
ORDER BY total_fiat_exposure DESC;

-- Card 3: Active Mule Hub Intercepts (Data Table - Conditional Formatting dark red)

SELECT tx_id, sender, receiver, amount, timestamp 
FROM transaction_audit 
WHERE is_mule = TRUE 
ORDER BY timestamp DESC 
LIMIT 20;
