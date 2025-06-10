import os
import utils

BLOCK_SIZE = 2**16
NUM_BLOCKS = 10

# Paths
QUERY_FILE = 'like-in-imdb.json'
DATA_DIR = 'metadata/imdb/'
DB_FILE = f'dbs/{os.path.basename(os.path.normpath(DATA_DIR))}.duckdb'

# Open DuckDB
con = utils.open_duckdb(db_path=DB_FILE, read_only=True, threads=os.cpu_count())

# For each table and column
for tn, cn in utils.load_imdb_string_cols(QUERY_FILE):
  print(f'Processing {tn}.{cn}..')
  
  # Check how many rows are available
  total_rows = con.execute(f"SELECT COUNT(*) FROM {tn} where {cn} ~ '^[[:ascii:]]+$'").fetchone()[0]
  if total_rows < BLOCK_SIZE:
    print(f"=> Not enough rows ({total_rows}) in {tn}.{cn} for even one full block.")
    continue
  
  # Determine the max number of full blocks we can extract
  max_possible_blocks = total_rows // BLOCK_SIZE
  blocks_to_extract = min(NUM_BLOCKS, max_possible_blocks)

  for i in range(blocks_to_extract):
    offset = i * BLOCK_SIZE
    query = f"""
      SELECT {cn}
      FROM {tn}
      WHERE {cn} ~ '^[[:print:]\t\n\r\x0b\f]+$'
      LIMIT {BLOCK_SIZE} OFFSET {offset}
    """
    block = con.execute(query).fetchall()
    
    with open(f'words/{tn}-{cn}-block-{i}.txt', 'w') as f:
      f.writelines('\n'.join(row[0] for row in block if row[0] is not None))

con.close()
print("✅ Block extraction complete.")
