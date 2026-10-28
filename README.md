# hm31_graphs
We analyze metadata use for citation graph clustering

Step_1: Working on Cluster 1 of CEN. We extract some features and report statistics for that cluster.

## Known issue: weighted clustering results (weight–edge misalignment)
The weighted Leiden runs in `step_10_cluster/` attach weights to the wrong edges. `leiden()` in `step_10_cluster/weighting_clustering_pipeline.py` passes the weight column (`pandas_df['weight']`) to `leidenalg` by position. The graph, however, is built through networkx (`construct_base_graph.py`, `ig.Graph.from_networkx`), which reorders the edges, so almost every weight lands on a different edge. Randomly permuted weights reproduce the weighted results; weights attached to their own edges do not.

- **Affected:** all weighted clustering results, including those in the M.S. thesis *A Framework for Similarity-Based Clustering of Biomedical Citation Graphs* (H. Mohasel Arjomandi, UIUC, 2024): the intuitive weights, the topological sweeps, and the [1,10] rescaled runs.
- **Not affected:** the unweighted clustering runs, the graph construction, and the edge features.
- **Fix:** attach the weights as an igraph edge attribute keyed by the endpoint pair, or build the graph directly from the weight table's rows.
- **Evidence and erratum:** https://github.com/HosseinMA96/pubmed-meta-etl (`edge_order_check/`, `scale_test/`, and the thesis erratum in its README).

## Database credentials
Scripts that connect to the database read the password from the `ERNIE_DB_PASSWORD` environment variable:

```bash
export ERNIE_DB_PASSWORD=...
```

## Data
Data files (`.npy`, `.pkl`, `.json`, `.csv`, `.tsv`, `.parquet`) are no longer tracked in this repository, because they derive from network data with data-use terms. The code is unchanged otherwise.
