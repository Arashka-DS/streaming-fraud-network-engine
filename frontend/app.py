import streamlit as st
import pandas as pd
import numpy as np
import psycopg2
import networkx as nx
from community import community_louvain
from pyvis.network import Network
import plotly.graph_objects as go
import streamlit.components.v1 as components
import os

st.set_page_config(page_title="FinTech Mule & Network Fraud SOC", layout="wide")

st.title("🛡️ Streaming Network Fraud & Mule Detection SOC")
st.caption("Real-Time Redpanda Streaming, Redis Sliding Velocity, Louvain Communities & Benford's Law Auditing")

# DB Connection
def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        database=os.getenv("DB_NAME", "fraud_warehouse"),
        user=os.getenv("DB_USER", "fraud_admin"),
        password=os.getenv("DB_PASSWORD", "fraud_password"),
        port=5432
    )

tab1, tab2, tab3 = st.tabs(["🕸️ Mule Network Graph", "📊 Benford's Law Audit", "🚨 Live Alert Ledger"])

# --- TAB 1: PyVis Interactive Network Graph ---
with tab1:
    st.subheader("Transactional Network Topology & Mule Rings")
    st.markdown("Visualizing directed fund flows. Red nodes indicate flagged **Mule Hubs** (balanced high-degree pass-throughs).")
    
    col_ctrl, col_graph = st.columns([1, 3])
    
    with col_ctrl:
        limit = st.slider("Transaction Window Sample", 50, 500, 150)
        min_amount = st.number_input("Minimum Transaction (IRR)", value=1000000)
        reload_btn = st.button("Recompute Graph", type="primary")

    try:
        conn = get_db_connection()
        query = f"""
            SELECT source_account, destination_account, amount, is_mule_candidate, is_anomaly
            FROM transaction_audit
            WHERE amount >= {min_amount}
            ORDER BY timestamp DESC
            LIMIT {limit};
        """
        df_tx = pd.read_sql(query, conn)
        conn.close()

        if df_tx.empty:
            st.info("No transaction data available. Run the synthetic producer to populate the database.")
        else:
            # Build NetworkX DiGraph
            G = nx.DiGraph()
            for _, row in df_tx.iterrows():
                src = str(row["source_account"])
                dst = str(row["destination_account"])
                amt = float(row["amount"])
                G.add_edge(src, dst, weight=amt)
                
                # Attribute tracking
                if "mule" not in G.nodes[dst]:
                    G.nodes[dst]["mule"] = row["is_mule_candidate"]
                if "mule" not in G.nodes[src]:
                    G.nodes[src]["mule"] = False

            # Louvain Community Partitioning (converted to undirected for modularity)
            undirected_G = G.to_undirected()
            partition = community_louvain.best_partition(undirected_G)

            # PyVis Visual Construction
            net = Network(height="600px", width="100%", bgcolor="#0E1117", font_color="#FFFFFF", directed=True)
            net.barnes_hut(gravity=-3000, central_gravity=0.3, spring_length=100, spring_strength=0.05)

            # Color palette for communities
            community_colors = ["#38BDF8", "#F59E0B", "#10B981", "#8B5CF6", "#EC4899", "#14B8A6"]

            for node in G.nodes():
                is_mule = G.nodes[node].get("mule", False)
                comm_id = partition.get(node, 0)
                color = "#EF4444" if is_mule else community_colors[comm_id % len(community_colors)]
                size = 25 if is_mule else 14
                label = f"🚨 MULE: {node}" if is_mule else f"User: {node}"
                net.add_node(node, label=label, color=color, size=size, title=f"Community: {comm_id}")

            for src, dst, data in G.edges(data=True):
                net.add_edge(src, dst, value=data["weight"], title=f"Amount: {data['weight']:,.0f} IRR")

            os.makedirs("frontend/temp", exist_ok=True)
            graph_html_path = "frontend/temp/network.html"
            net.save_graph(graph_html_path)

            with col_graph:
                with open(graph_html_path, "r", encoding="utf-8") as f:
                    components.html(f.read(), height=620)
    except Exception as e:
        st.error(f"Database/Graph Rendering Error: {e}")

# --- TAB 2: Benford's Law Audit ---
with tab2:
    st.subheader("First-Digit Anomaly Detection (Benford's Law)")
    st.markdown("Natural financial flows conform to $P(d) = \\log_{10}(1 + 1/d)$. Deviations flag synthetic script injection or smurfing.")

    try:
        conn = get_db_connection()
        query_digits = "SELECT amount FROM transaction_audit WHERE amount > 0 LIMIT 2000;"
        amounts_df = pd.read_sql(query_digits, conn)
        conn.close()

        if not amounts_df.empty:
            # Extract first leading digits
            first_digits = amounts_df["amount"].astype(str).str.extract(r'([1-9])')[0].astype(int)
            counts = first_digits.value_counts().reindex(range(1, 10), fill_value=0)
            empirical_probs = counts / counts.sum()

            # Theoretical Benford distribution
            digits = np.arange(1, 10)
            benford_probs = np.log10(1 + 1 / digits)

            # Plotly Dual-Line Chart
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=digits, y=empirical_probs, name="Observed Ingestion",
                marker_color="#38BDF8", opacity=0.7
            ))
            fig.add_trace(go.Scatter(
                x=digits, y=benford_probs, mode="lines+markers", name="Theoretical Benford",
                line=dict(color="#F59E0B", width=3)
            ))
            fig.update_layout(
                template="plotly_dark",
                xaxis=dict(title="Leading Significant Digit", tickmode="linear"),
                yaxis=dict(title="Relative Probability", range=[0, 0.4]),
                height=450
            )

            c1, c2 = st.columns([2.5, 1])
            with c1:
                st.plotly_chart(fig, use_container_width=True)
            with c2:
                # KS Distance
                ks_anomaly_score = np.max(np.abs(np.cumsum(empirical_probs) - np.cumsum(benford_probs)))
                st.metric("KS Divergence", f"{ks_anomaly_score:.4f}")
                if ks_anomaly_score > 0.05:
                    st.error("🚨 CRITICAL: Benford anomaly threshold breached (> 0.05). Synthetic transaction generation detected.")
                else:
                    st.success("✅ Distribution within standard Benford bounds.")
        else:
            st.info("Awaiting transaction data for Benford statistical analysis.")
    except Exception as e:
        st.error(f"Benford Audit Error: {e}")

# --- TAB 3: Live Fraud Alert Ledger ---
with tab3:
    st.subheader("Persisted Anomaly & Mule Audit Ledger")
    try:
        conn = get_db_connection()
        alerts_df = pd.read_sql("""
            SELECT transaction_id, source_account, destination_account, amount, 
                   velocity_score, is_anomaly, is_mule_candidate, timestamp
            FROM transaction_audit
            WHERE is_anomaly = TRUE OR is_mule_candidate = TRUE
            ORDER BY timestamp DESC
            LIMIT 50;
        """, conn)
        conn.close()

        if alerts_df.empty:
            st.info("No flagged anomalies recorded yet.")
        else:
            st.dataframe(alerts_df, use_container_width=True)
    except Exception as e:
        st.error(f"Audit Log Error: {e}")
