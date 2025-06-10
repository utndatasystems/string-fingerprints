# Generation

## Setup

Download IMDb data to `data`:

```
wget https://event.cwi.nl/da/job/imdb.tgz data/
```

Unpack the zipped file in `data`:

```
mkdir data/imdb
tar -xvf data/imdb.tgz -C data/imdb
```

Load the data into `duckdb`:

```
python data-load.py
```

## Data

Once the data has been loaded, we can generate data blocks:

```
python data-gen.py
```

## Queries

To generate queries:

```
python query-gen.py
```