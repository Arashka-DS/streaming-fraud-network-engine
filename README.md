# Streaming Network Fraud & Mule Detection Engine

An enterprise-grade anti-money laundering (AML) and fraud detection platform. This pipeline processes streaming transactions to identify synthetic behaviors, localized anomalies, and organized mule rings using a multi-layered detection architecture.

## Detection Taxonomy
1. **Row-Level ML:** `IsolationForest` detects statistical outliers based on transaction size and rolling velocity.
2. **Graph Heuristics:** Tracks directed money flows using `NetworkX` to deterministically flag Mule Hubs (high in/out degree with balanced volume).
3. **Macro Community Detection:** Applies the **Louvain Method** to segment the transaction graph into localized fraud rings.
4. **Audit Level:** Continuously audits global transaction distributions against **Benford's Law** $P(d) = \log_{10}\left(1 + \frac{1}{d}\right)$ to identify synthetic/bot-generated fiat volumes.

## Architecture Stack
- **Streaming Ingestion:** Redpanda (Kafka-compatible).
- **Feature Store:** Redis (Sub-millisecond sliding window `ZSET` velocity aggregations).
- **Inference & Graph:** Python, Scikit-Learn, NetworkX, python-louvain.
- **Persistence & BI:** PostgreSQL for immutable audit logging, Metabase for executive reporting.

## Running the Engine
1. Spin up the cluster: `docker-compose up -d --build`
2. Start the synthetic transaction producer: `python src/producer.py`
3. Access the Benford's Law Audit endpoint at `http://localhost:8000/metrics/benford`.
4. Open Metabase at `http://localhost:3000` to visualize the PostgreSQL `transaction_audit` ledger.
