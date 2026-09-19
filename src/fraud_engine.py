import networkx as nx
import community as community_louvain # python-louvain
from pyvis.network import Network
from sklearn.ensemble import IsolationForest
import pandas as pd
import numpy as np
import math

class FraudGraphEngine:
    def __init__(self):
        self.G = nx.DiGraph()
        self.transaction_amounts = [] # Stored in-memory for Benford's Law
        
        np.random.seed(42)
        dummy_data = pd.DataFrame({
            'amount': np.random.lognormal(mean=3, sigma=1, size=1000),
            'tx_count_1h': np.random.poisson(lam=2, size=1000)
        })
        self.iso_forest = IsolationForest(contamination=0.05, random_state=42)
        self.iso_forest.fit(dummy_data)

    def process_transaction(self, sender, receiver, amount, velocity_count):
        features = pd.DataFrame([{'amount': amount, 'tx_count_1h': velocity_count}])
        is_anomaly = self.iso_forest.predict(features)[0] == -1

        self.transaction_amounts.append(amount)

        if self.G.has_edge(sender, receiver):
            self.G[sender][receiver]['weight'] += amount
        else:
            self.G.add_edge(sender, receiver, weight=amount)

        in_deg = self.G.in_degree(sender)
        out_deg = self.G.out_degree(sender)
        in_vol = sum(d['weight'] for u, v, d in self.G.in_edges(sender, data=True))
        out_vol = sum(d['weight'] for u, v, d in self.G.out_edges(sender, data=True))
        
        is_mule = (in_deg >= 2) and (out_deg >= 2) and (0.8 <= (in_vol / (out_vol + 1e-5)) <= 1.2)

        if is_mule or is_anomaly:
            self.G.nodes[sender]['color'] = '#FF1744'
            self.G.nodes[sender]['title'] = 'Flagged: MULE/ANOMALY'
        else:
            self.G.nodes[sender]['color'] = '#29B6F6'

        return {"is_anomaly": is_anomaly, "is_mule": is_mule}

    def compute_louvain_communities(self):
        """Detects organized rings by finding densely connected graph communities."""
        if len(self.G) == 0:
            return {}
        # Louvain requires an undirected graph
        undirected_G = self.G.to_undirected()
        partition = community_louvain.best_partition(undirected_G, weight='weight')
        return partition

    def check_benfords_law(self):
        """Audits all transactions against Benford's logarithmic distribution."""
        if len(self.transaction_amounts) < 100:
            return {"status": "insufficient_data"}
            
        first_digits = [int(str(amount)[0]) for amount in self.transaction_amounts if amount >= 1]
        actual_counts = {i: first_digits.count(i) for i in range(1, 10)}
        total = sum(actual_counts.values())
        
        actual_dist = {str(k): v / total for k, v in actual_counts.items()}
        # Benford's Law Formula: P(d) = log10(1 + 1/d)
        expected_dist = {str(d): math.log10(1 + 1/d) for d in range(1, 10)}
        
        return {"actual_distribution": actual_dist, "expected_distribution": expected_dist}
