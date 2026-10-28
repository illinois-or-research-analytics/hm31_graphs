# hm31_graphs
We analyze metadata use for citation graph clustering

Step_1: Working on Cluster 1 of CEN. We extract some features and report statistics for that cluster.

## Database credentials
Scripts that connect to the database read the password from the `ERNIE_DB_PASSWORD` environment variable:

```bash
export ERNIE_DB_PASSWORD=...
```

## Data
Data files (`.npy`, `.pkl`, `.json`, `.csv`, `.tsv`, `.parquet`) are no longer tracked in this repository, because they derive from network data with data-use terms. The code is unchanged otherwise.
