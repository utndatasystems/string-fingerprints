# Instance-Optimized String Fingerprints

String fingerprints act as a secondary index to speed your table scans. If summarized at block level, they can be used as zonemaps to reduce I/O.

# Setup

```
pip install -r requirements.txt
```

# Instance Optimization

To optimize the string fingerprints, we use a MIP formulation, found in `optimizer`.

Some already-optimized partitions for IMDb's `title` column can be found in `cache`.

# Paper Plots

## `FPR` Plot

To create the `FPR` plot, run the following:

```
python run-fpr.py
```

## Runtime Plot

To get the runtimes, run the following:

```
python run-speedup.py results/title-title 0
```

The prepared queries are stored into `prepared-queries`. Now, run the queries using the following:

```
./run-prepared.sh ~/.duckdb/cli/1.3.0/duckdb ./prepared-queries
```
