# Streaming Network Fraud & Mule Detection Engine

An enterprise-grade, event-driven anti-money laundering (AML) platform designed to detect synthetic transaction injection, localized velocity spikes, and organized mule accounts in real time.

## 🏛️ Detection Taxonomy
1. **Row-Level ML (Isolation Forest):** Evaluates incoming transaction volumes and rolling transaction frequency to score statistical anomalies.
2. **In-Memory Velocity Tracking (Redis):** Tracks sliding-window account velocity ($t-60\text{s}$) using Redis `ZSET` aggregations for sub-millisecond lookups.
3. **Graph Topology & Mule Hub Detection (NetworkX):** Evaluates directed money flows to identify accounts with high in/out degree parity operating as high-throughput pass-through conduits.
4. **Macro Community Detection (Louvain Method):** Dynamically partitions the global transaction graph into modularity-optimized clusters to isolate coordinated fraud rings.
5. **Macro Audit (Benford's Law):** Evaluates empirical leading-digit distributions against $P(d) = \log_{10}(1 + 1/d)$ via Kolmogorov-Smirnov statistical divergence tests to detect automated wash-trading scripts.

## ⚙️ Architectural Stack
* **Event Ingestion:** Redpanda (Kafka-compatible event broker)
* **Real-Time State:** Redis (In-memory sorted sets for sliding window counts)
* **Storage & Audit:** PostgreSQL (Immutable compliance ledger)
* **Analysis & ML:** Python, Scikit-Learn, NetworkX, python-louvain, Scipy
* **Dashboards:** Streamlit (SOC UI with interactive PyVis network graphs) & Metabase (Operational BI)

## 🚀 Quick Start
1. **Boot the complete infrastructure:**
   ```bash
   docker-compose up -d --build
   ```
2. **Launch the synthetic fraud & mule generator:**
   ```bash
   python src/producer.py
   ```
3. **Access the Interfaces:**
   * **Streamlit SOC Dashboard:** `http://localhost:8501` (Interactive PyVis graph, Benford curve, live alerts)
   * **FastAPI Docs & Metrics:** `http://localhost:8000/docs`
   * **Metabase BI:** `http://localhost:3000` (Pre-configured for `fraud_warehouse`)
