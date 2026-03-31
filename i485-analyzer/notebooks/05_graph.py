#!/usr/bin/env python3
"""
I-485 Fraud Detection — Graph Analytics Pipeline

Builds a heterogeneous graph from I-485 application data, computes graph-theoretic
features (centrality, PageRank, Louvain communities), trains Node2Vec embeddings,
and uses HDBSCAN to detect fraud ring clusters.

Usage:
    python notebooks/05_graph.py

Inputs:
    synthetic_data/i485_form/parquet/  — application, applicant_info, addresses,
                                         contacts_signatures tables
    data/features/labels.parquet       — binary fraud labels (application_id, is_fraud)

Outputs:
    data/results/graph_features.parquet  — per-app graph + embedding features
    data/results/fraud_rings.parquet     — HDBSCAN fraud ring clusters
    data/models/node2vec.model           — trained Node2Vec (gensim) model
"""

import json
import logging
import os
import sys
import time
import warnings
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore", category=FutureWarning)

# ── Project root (two levels up from this script) ────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

PARQUET_DIR = PROJECT_ROOT / "synthetic_data" / "i485_form" / "parquet"
LABELS_PATH = PROJECT_ROOT / "data" / "features" / "labels.parquet"
RESULTS_DIR = PROJECT_ROOT / "data" / "results"
MODELS_DIR = PROJECT_ROOT / "data" / "models"

for _d in (RESULTS_DIR, MODELS_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# ── Logging ──────────────────────────────────────────────────────────────────
LOG = logging.getLogger("graph_analytics")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)

# ── Optional dependency flags ────────────────────────────────────────────────
try:
    import networkx as nx
except ImportError:
    LOG.error("networkx is required. Install: pip install networkx")
    sys.exit(1)

HAS_COMMUNITY = False
try:
    import community as community_louvain  # python-louvain
    HAS_COMMUNITY = True
except ImportError:
    LOG.warning("python-louvain not installed — Louvain community detection "
                "will be skipped. Install: pip install python-louvain")

HAS_NODE2VEC = False
try:
    from node2vec import Node2Vec
    HAS_NODE2VEC = True
except ImportError:
    LOG.warning("node2vec not installed — embedding step will be skipped. "
                "Install: pip install node2vec")

HAS_HDBSCAN = False
try:
    import hdbscan
    HAS_HDBSCAN = True
except ImportError:
    LOG.warning("hdbscan not installed — clustering step will be skipped. "
                "Install: pip install hdbscan")

N2V_DIMS = 64


# =============================================================================
#  1 — DATA LOADING
# =============================================================================

def load_parquet(name: str) -> pd.DataFrame:
    """Load a Parquet table from the synthetic data directory."""
    path = PARQUET_DIR / f"{name}.parquet"
    if not path.exists():
        LOG.error(f"Missing table: {path}")
        sys.exit(1)
    df = pd.read_parquet(path)
    LOG.info(f"  Loaded {name}: {len(df):,} rows, {len(df.columns)} cols")
    return df


def load_data():
    """Load all required tables and the fraud labels."""
    LOG.info("Loading input tables ...")
    t0 = time.time()

    tables = {}
    tables["application"] = load_parquet("application")
    tables["applicant_info"] = load_parquet("applicant_info")
    tables["addresses"] = load_parquet("addresses")
    tables["contacts_signatures"] = load_parquet("contacts_signatures")

    # Labels
    if LABELS_PATH.exists():
        labels = pd.read_parquet(LABELS_PATH)
        LOG.info(f"  Loaded labels: {len(labels):,} rows")
    else:
        LOG.warning(f"Labels file not found at {LABELS_PATH} — "
                    "neighbour fraud rate / community fraud density will be 0")
        labels = pd.DataFrame(columns=["application_id", "is_fraud"])

    LOG.info(f"Data loaded in {time.time() - t0:.1f}s")
    return tables, labels


# =============================================================================
#  2 — GRAPH CONSTRUCTION
# =============================================================================

def build_graph(tables, labels):
    """Build an undirected heterogeneous graph with typed nodes and edges."""
    LOG.info("Building graph ...")
    t0 = time.time()

    G = nx.Graph()

    app_df = tables["application"]
    info_df = tables["applicant_info"]
    addr_df = tables["addresses"]
    cs_df = tables["contacts_signatures"]

    app_ids = app_df["application_id"].values
    n_apps = len(app_ids)

    # Build fraud lookup for later
    fraud_lookup = {}
    if len(labels) > 0 and "is_fraud" in labels.columns:
        fraud_lookup = dict(zip(
            labels["application_id"].values,
            labels["is_fraud"].astype(int).values,
        ))

    # ── APP nodes ────────────────────────────────────────────────────────
    LOG.info(f"  Adding {n_apps:,} APP nodes ...")
    for aid in app_ids:
        G.add_node(f"APP_{aid}", node_type="APP",
                    is_fraud=fraud_lookup.get(aid, 0))

    # ── SSN nodes + edges ────────────────────────────────────────────────
    LOG.info("  Adding SSN nodes + HAS_SSN edges ...")
    ssn_col = info_df[["application_id", "ssn"]].dropna(subset=["ssn"])
    ssn_col = ssn_col[ssn_col["ssn"].astype(str).str.strip() != ""]
    for aid, ssn in zip(ssn_col["application_id"].values,
                        ssn_col["ssn"].values):
        ssn_node = f"SSN_{ssn}"
        if not G.has_node(ssn_node):
            G.add_node(ssn_node, node_type="SSN")
        G.add_edge(f"APP_{aid}", ssn_node, edge_type="HAS_SSN")

    # ── ANUM nodes + edges ───────────────────────────────────────────────
    LOG.info("  Adding ANUM nodes + HAS_ANUM edges ...")
    anum_col = app_df[["application_id", "a_number"]].dropna(subset=["a_number"])
    anum_col = anum_col[anum_col["a_number"].astype(str).str.strip() != ""]
    for aid, anum in zip(anum_col["application_id"].values,
                         anum_col["a_number"].values):
        anum_node = f"ANUM_{anum}"
        if not G.has_node(anum_node):
            G.add_node(anum_node, node_type="ANUM")
        G.add_edge(f"APP_{aid}", anum_node, edge_type="HAS_ANUM")

    # ── ADDR nodes + edges (current addresses only) ──────────────────────
    LOG.info("  Adding ADDR nodes + LIVES_AT edges ...")
    addr_current = addr_df[
        addr_df["address_type"].astype(str).str.upper().str.contains("CURRENT")
    ].copy()
    addr_current["state_str"] = addr_current["state"].fillna("").astype(str).str.strip()
    addr_current["city_str"] = addr_current["city"].fillna("").astype(str).str.strip()
    addr_current = addr_current[
        (addr_current["state_str"] != "") | (addr_current["city_str"] != "")
    ]
    for aid, st, ct in zip(addr_current["application_id"].values,
                           addr_current["state_str"].values,
                           addr_current["city_str"].values):
        addr_node = f"ADDR_{st}|{ct}"
        if not G.has_node(addr_node):
            G.add_node(addr_node, node_type="ADDR")
        G.add_edge(f"APP_{aid}", addr_node, edge_type="LIVES_AT")

    # ── PREP nodes + edges ───────────────────────────────────────────────
    LOG.info("  Adding PREP nodes + PREPARED_BY edges ...")
    prep_df = cs_df[
        cs_df["contact_type"].astype(str).str.upper().str.contains("PREPARER")
    ].copy()
    prep_df["prep_name"] = (
        prep_df["family_name"].fillna("").astype(str).str.strip()
        + " "
        + prep_df["given_name"].fillna("").astype(str).str.strip()
    ).str.strip()
    prep_df = prep_df[prep_df["prep_name"] != ""]
    for aid, pname in zip(prep_df["application_id"].values,
                          prep_df["prep_name"].values):
        prep_node = f"PREP_{pname}"
        if not G.has_node(prep_node):
            G.add_node(prep_node, node_type="PREP")
        G.add_edge(f"APP_{aid}", prep_node, edge_type="PREPARED_BY")

    # ── ATTY nodes + edges ───────────────────────────────────────────────
    LOG.info("  Adding ATTY nodes + REPRESENTED_BY edges ...")
    atty_col = app_df[["application_id", "atty_state_bar_number"]].dropna(
        subset=["atty_state_bar_number"]
    )
    atty_col = atty_col[
        atty_col["atty_state_bar_number"].astype(str).str.strip() != ""
    ]
    for aid, bar in zip(atty_col["application_id"].values,
                        atty_col["atty_state_bar_number"].values):
        atty_node = f"ATTY_{bar}"
        if not G.has_node(atty_node):
            G.add_node(atty_node, node_type="ATTY")
        G.add_edge(f"APP_{aid}", atty_node, edge_type="REPRESENTED_BY")

    # ── APP ↔ APP edges (SHARES_NAME_DOB) ────────────────────────────────
    LOG.info("  Adding SHARES_NAME_DOB edges ...")
    nd = info_df[["application_id", "family_name", "given_name",
                   "date_of_birth"]].copy()
    nd = nd.dropna(subset=["family_name", "given_name", "date_of_birth"])
    nd["key"] = (
        nd["family_name"].astype(str).str.upper()
        + "|"
        + nd["given_name"].astype(str).str.upper()
        + "|"
        + nd["date_of_birth"].astype(str)
    )
    # Group by key — only keys with >1 app create edges
    nd_groups = nd.groupby("key")["application_id"].apply(list)
    nd_groups = nd_groups[nd_groups.apply(len) > 1]

    # Limit: if a key has very many matches (>50), skip it to avoid
    # combinatorial explosion (likely a data quality issue, not a real pattern)
    MAX_GROUP = 50
    name_dob_edges = 0
    for _key, aid_list in nd_groups.items():
        if len(aid_list) > MAX_GROUP:
            continue
        for i in range(len(aid_list)):
            for j in range(i + 1, len(aid_list)):
                n1 = f"APP_{aid_list[i]}"
                n2 = f"APP_{aid_list[j]}"
                if G.has_node(n1) and G.has_node(n2):
                    G.add_edge(n1, n2, edge_type="SHARES_NAME_DOB")
                    name_dob_edges += 1
    LOG.info(f"    SHARES_NAME_DOB edges added: {name_dob_edges:,}")

    elapsed = time.time() - t0
    LOG.info(f"  Graph built in {elapsed:.1f}s")
    LOG.info(f"  Nodes: {G.number_of_nodes():,}  |  Edges: {G.number_of_edges():,}")
    LOG.info(f"  Connected components: {nx.number_connected_components(G):,}")

    return G, fraud_lookup


# =============================================================================
#  3 — GRAPH FEATURES
# =============================================================================

def compute_graph_features(G, fraud_lookup, app_ids):
    """Compute graph-theoretic features for each APP node."""
    LOG.info("Computing graph features ...")
    t0 = time.time()

    app_nodes = [f"APP_{aid}" for aid in app_ids]
    app_node_set = set(app_nodes)

    # ── Degree centrality ────────────────────────────────────────────────
    LOG.info("  degree_centrality ...")
    deg_cent = nx.degree_centrality(G)

    # ── Betweenness centrality (sampled) ─────────────────────────────────
    num_nodes = G.number_of_nodes()
    k_sample = min(5000, num_nodes)
    LOG.info(f"  betweenness_centrality (k={k_sample}) ...")
    bet_cent = nx.betweenness_centrality(G, k=k_sample)

    # ── PageRank ─────────────────────────────────────────────────────────
    LOG.info("  pagerank ...")
    pr = nx.pagerank(G)

    # ── Clustering coefficient ───────────────────────────────────────────
    LOG.info("  clustering_coefficient ...")
    clust = nx.clustering(G)

    # ── Connected component sizes ────────────────────────────────────────
    LOG.info("  connected_component_size ...")
    comp_size = {}
    for comp in nx.connected_components(G):
        sz = len(comp)
        for node in comp:
            comp_size[node] = sz

    # ── Neighbour fraud rate (APP-type neighbours only) ──────────────────
    LOG.info("  neighbor_fraud_rate ...")
    nbr_fraud = {}
    for node in app_nodes:
        neighbours = list(G.neighbors(node))
        app_neighbours = [n for n in neighbours if n in app_node_set]
        if len(app_neighbours) == 0:
            nbr_fraud[node] = 0.0
        else:
            fraud_count = sum(
                fraud_lookup.get(int(n.replace("APP_", "")), 0)
                for n in app_neighbours
            )
            nbr_fraud[node] = fraud_count / len(app_neighbours)

    # ── Louvain communities ──────────────────────────────────────────────
    community_map = {}
    community_fraud_density = {}
    community_sizes = {}
    if HAS_COMMUNITY:
        LOG.info("  Louvain community detection ...")
        partition = community_louvain.best_partition(G)
        n_communities = len(set(partition.values()))
        LOG.info(f"    Louvain communities found: {n_communities:,}")

        # Compute per-community fraud density and size (APP nodes only)
        comm_apps = defaultdict(list)  # community_id -> [app_id, ...]
        for node, cid in partition.items():
            if node.startswith("APP_"):
                aid = int(node.replace("APP_", ""))
                comm_apps[cid].append(aid)

        comm_fraud_dens = {}
        comm_sz = {}
        for cid, aids in comm_apps.items():
            fraud_cnt = sum(fraud_lookup.get(a, 0) for a in aids)
            comm_fraud_dens[cid] = fraud_cnt / len(aids) if aids else 0.0
            comm_sz[cid] = len(aids)

        for node in app_nodes:
            cid = partition.get(node, -1)
            community_map[node] = cid
            community_fraud_density[node] = comm_fraud_dens.get(cid, 0.0)
            community_sizes[node] = comm_sz.get(cid, 0)
    else:
        for node in app_nodes:
            community_map[node] = -1
            community_fraud_density[node] = 0.0
            community_sizes[node] = 0

    # ── Assemble feature DataFrame ───────────────────────────────────────
    LOG.info("  Assembling graph feature matrix ...")
    records = []
    for aid in app_ids:
        node = f"APP_{aid}"
        records.append({
            "application_id": aid,
            "degree_centrality": deg_cent.get(node, 0.0),
            "betweenness_centrality": bet_cent.get(node, 0.0),
            "pagerank": pr.get(node, 0.0),
            "connected_component_size": comp_size.get(node, 1),
            "clustering_coefficient": clust.get(node, 0.0),
            "neighbor_fraud_rate": nbr_fraud.get(node, 0.0),
            "community_id": community_map.get(node, -1),
            "community_fraud_density": community_fraud_density.get(node, 0.0),
            "community_size": community_sizes.get(node, 0),
        })

    features_df = pd.DataFrame(records)

    elapsed = time.time() - t0
    LOG.info(f"  Graph features computed in {elapsed:.1f}s")
    return features_df


# =============================================================================
#  4 — NODE2VEC EMBEDDINGS
# =============================================================================

def train_node2vec(G, app_ids):
    """Train Node2Vec embeddings and return a 64-dim vector per APP node."""
    if not HAS_NODE2VEC:
        LOG.warning("Skipping Node2Vec (not installed)")
        return None, None

    LOG.info("Training Node2Vec embeddings ...")
    t0 = time.time()

    num_nodes = G.number_of_nodes()
    num_walks = 200 if num_nodes <= 500_000 else 50
    LOG.info(f"  Nodes: {num_nodes:,} | num_walks={num_walks}")

    node2vec_model = Node2Vec(
        G,
        dimensions=N2V_DIMS,
        walk_length=30,
        num_walks=num_walks,
        p=1,
        q=0.5,
        workers=4,
    )
    model = node2vec_model.fit(window=10, min_count=1)

    elapsed = time.time() - t0
    LOG.info(f"  Node2Vec trained in {elapsed:.1f}s")

    # Save model
    model_path = str(MODELS_DIR / "node2vec.model")
    model.save(model_path)
    LOG.info(f"  Model saved: {model_path}")

    # Extract embeddings for APP nodes
    embeddings = {}
    for aid in app_ids:
        node = f"APP_{aid}"
        if node in model.wv:
            embeddings[aid] = model.wv[node]
        else:
            embeddings[aid] = np.zeros(N2V_DIMS)

    return embeddings, model


# =============================================================================
#  5 — HDBSCAN CLUSTERING
# =============================================================================

def cluster_embeddings(embeddings, fraud_lookup, app_ids):
    """Cluster Node2Vec embeddings with HDBSCAN and identify fraud rings."""
    if not HAS_HDBSCAN or embeddings is None:
        LOG.warning("Skipping HDBSCAN clustering (dependency missing or no embeddings)")
        return None

    LOG.info("Running HDBSCAN clustering on embeddings ...")
    t0 = time.time()

    # Build embedding matrix in app_ids order
    emb_matrix = np.array([embeddings.get(aid, np.zeros(N2V_DIMS)) for aid in app_ids])

    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=5,
        min_samples=3,
        cluster_selection_method="eom",
    )
    cluster_labels = clusterer.fit_predict(emb_matrix)

    # Build per-cluster stats
    cluster_info = defaultdict(lambda: {"members": [], "fraud_count": 0})
    for i, aid in enumerate(app_ids):
        cid = int(cluster_labels[i])
        if cid == -1:
            continue  # noise
        cluster_info[cid]["members"].append(int(aid))
        cluster_info[cid]["fraud_count"] += fraud_lookup.get(aid, 0)

    # Build fraud_rings DataFrame
    ring_records = []
    fraud_ring_count = 0
    suspect_apps = set()

    for cid, info in sorted(cluster_info.items()):
        members = info["members"]
        fraud_cnt = info["fraud_count"]
        size = len(members)
        density = fraud_cnt / size if size > 0 else 0.0
        is_ring = density > 0.3

        if is_ring:
            fraud_ring_count += 1
            # Flag unflagged members as suspects
            for aid in members:
                if fraud_lookup.get(aid, 0) == 0:
                    suspect_apps.add(aid)

        ring_records.append({
            "cluster_id": cid,
            "cluster_size": size,
            "fraud_density": round(density, 4),
            "fraud_count": fraud_cnt,
            "member_app_ids": json.dumps(members),
            "is_fraud_ring": is_ring,
        })

    rings_df = pd.DataFrame(ring_records)

    n_clusters = len(cluster_info)
    elapsed = time.time() - t0
    LOG.info(f"  HDBSCAN complete in {elapsed:.1f}s")
    LOG.info(f"  Clusters found: {n_clusters:,}")
    LOG.info(f"  Fraud rings identified (density > 30%): {fraud_ring_count:,}")
    LOG.info(f"  Suspect apps (unflagged in fraud rings): {len(suspect_apps):,}")

    return rings_df, cluster_labels


# =============================================================================
#  6 — OUTPUT ASSEMBLY
# =============================================================================

def save_outputs(features_df, embeddings, rings_result, app_ids):
    """Merge graph features + embeddings, save all output files."""
    LOG.info("Saving output files ...")

    # Attach embedding columns to features
    if embeddings is not None:
        emb_cols = [f"n2v_{i}" for i in range(N2V_DIMS)]
        emb_rows = []
        for aid in app_ids:
            vec = embeddings.get(aid, np.zeros(N2V_DIMS))
            emb_rows.append(vec)
        emb_df = pd.DataFrame(emb_rows, columns=emb_cols)
        emb_df["application_id"] = app_ids
        features_df = features_df.merge(emb_df, on="application_id", how="left")
    else:
        # Fill with zeros if no embeddings
        for i in range(N2V_DIMS):
            features_df[f"n2v_{i}"] = 0.0

    # Save graph features
    gf_path = RESULTS_DIR / "graph_features.parquet"
    features_df.to_parquet(str(gf_path), index=False, engine="pyarrow")
    LOG.info(f"  graph_features.parquet: {len(features_df):,} rows, "
             f"{len(features_df.columns)} cols -> {gf_path}")

    # Save fraud rings
    if rings_result is not None:
        rings_df, _ = rings_result
        fr_path = RESULTS_DIR / "fraud_rings.parquet"
        rings_df.to_parquet(str(fr_path), index=False, engine="pyarrow")
        LOG.info(f"  fraud_rings.parquet: {len(rings_df):,} rows -> {fr_path}")
    else:
        LOG.info("  fraud_rings.parquet: skipped (no clustering)")

    return features_df


# =============================================================================
#  7 — CONSOLE SUMMARY
# =============================================================================

def print_summary(G, features_df, rings_result, n2v_time=None):
    """Print a summary report to the console."""
    print()
    print("=" * 70)
    print("  I-485 GRAPH ANALYTICS PIPELINE — SUMMARY")
    print("=" * 70)

    # Graph stats
    n_nodes = G.number_of_nodes()
    n_edges = G.number_of_edges()
    n_comps = nx.number_connected_components(G)
    print(f"\n  Graph:")
    print(f"    Nodes:                {n_nodes:>12,}")
    print(f"    Edges:                {n_edges:>12,}")
    print(f"    Connected components: {n_comps:>12,}")

    # Node type breakdown
    type_counts = defaultdict(int)
    for _, data in G.nodes(data=True):
        type_counts[data.get("node_type", "UNKNOWN")] += 1
    print(f"\n  Node types:")
    for ntype in ["APP", "SSN", "ANUM", "ADDR", "PREP", "ATTY"]:
        if ntype in type_counts:
            print(f"    {ntype:6s}  {type_counts[ntype]:>10,}")

    # Louvain
    if HAS_COMMUNITY and "community_id" in features_df.columns:
        n_comms = features_df["community_id"].nunique()
        print(f"\n  Louvain communities: {n_comms:,}")

    # Node2Vec
    if n2v_time is not None:
        print(f"  Node2Vec training time: {n2v_time:.1f}s")

    # HDBSCAN
    if rings_result is not None:
        rings_df, _ = rings_result
        n_clusters = len(rings_df)
        n_rings = rings_df["is_fraud_ring"].sum()
        print(f"\n  HDBSCAN:")
        print(f"    Clusters found:       {n_clusters:>8,}")
        print(f"    Fraud rings (>30%):   {n_rings:>8,}")

    # Top 10 PageRank APP nodes
    top_pr = features_df.nlargest(10, "pagerank")[
        ["application_id", "pagerank", "degree_centrality",
         "connected_component_size", "neighbor_fraud_rate"]
    ]
    print(f"\n  Top 10 highest PageRank APP nodes:")
    print(f"  {'app_id':>12s}  {'pagerank':>10s}  {'deg_cent':>10s}  "
          f"{'comp_size':>10s}  {'nbr_fraud':>10s}")
    print(f"  {'-'*12}  {'-'*10}  {'-'*10}  {'-'*10}  {'-'*10}")
    for _, row in top_pr.iterrows():
        print(f"  {int(row['application_id']):>12d}  "
              f"{row['pagerank']:>10.6f}  "
              f"{row['degree_centrality']:>10.6f}  "
              f"{int(row['connected_component_size']):>10d}  "
              f"{row['neighbor_fraud_rate']:>10.4f}")

    print()
    print("=" * 70)


# =============================================================================
#  MAIN
# =============================================================================

def main():
    t_start = time.time()

    print("=" * 70)
    print("  I-485 FRAUD DETECTION — GRAPH ANALYTICS PIPELINE")
    print("=" * 70)

    # 1. Load data
    tables, labels = load_data()

    app_ids = tables["application"]["application_id"].values

    # 2. Build graph
    G, fraud_lookup = build_graph(tables, labels)

    # 3. Compute graph features
    features_df = compute_graph_features(G, fraud_lookup, app_ids)

    # 4. Node2Vec embeddings
    n2v_t0 = time.time()
    n2v_result = train_node2vec(G, app_ids)
    n2v_time = time.time() - n2v_t0
    if n2v_result is not None:
        embeddings, n2v_model = n2v_result
    else:
        embeddings = None

    # 5. HDBSCAN clustering
    if embeddings is not None:
        rings_result = cluster_embeddings(embeddings, fraud_lookup, app_ids)
    else:
        rings_result = None

    # 6. Save outputs
    save_outputs(features_df, embeddings, rings_result, app_ids)

    # 7. Summary
    print_summary(G, features_df, rings_result,
                  n2v_time=n2v_time if HAS_NODE2VEC else None)

    total = time.time() - t_start
    print(f"  Total pipeline time: {total / 60:.1f} minutes")
    print(f"  Done.\n")


if __name__ == "__main__":
    main()
