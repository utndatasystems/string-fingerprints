import duckdb
import pandas as pd
import nutella
import utils
import os
from typing import List

NUM_THREADS = 1
OUTPUT_DIR = 'query-log'
QUERY_DIR = 'prepared-queries'

INIT_DUCKDB_SQL = f'''
  DROP TABLE IF EXISTS tab;
  CREATE TABLE tab (
    raw TEXT
  );
'''

INIT_NUTELLA_SQL = f'''
  DROP TABLE IF EXISTS tab;
  CREATE TABLE tab (
    raw TEXT,
    nutella INTEGER,
    helper BOOLEAN
  );
'''

class NutellaFingerprint:
  def __init__(self, partition):
    # Fetch the byte mapping.
    self.byte_mapping = nutella.fetch_byte_mapping(None, partition)

  def __call__(self, value: str) -> int:
    return nutella.build_fingerprint(value, self.byte_mapping)

class QueryWrapper:
  def __init__(self, competitor, tn, cn, config_name, workload_type, time_limit=None):
    self.competitor = competitor
    self.tn = tn
    self.cn = cn
    self.config_name = config_name
    self.workload_type = workload_type
    self.time_limit = time_limit

  def create_dir(self):
    # Specify the directory path.
    profile_dir = None
    if self.competitor in ['duckdb', 'naive']:
      assert self.time_limit is None
      profile_dir = os.path.join(
        OUTPUT_DIR,
        f'{self.tn}-{self.cn}',
        self.config_name,
        self.competitor,
        self.workload_type
      )
    else:
      assert self.time_limit is not None
      profile_dir = os.path.join(
        OUTPUT_DIR,
        f'{self.tn}-{self.cn}',
        self.config_name,
        self.competitor,
        self.workload_type,
        f'{self.time_limit}'
      )

    # Make sure the directory exists.
    assert profile_dir is not None
    os.makedirs(profile_dir, exist_ok=True)
    return profile_dir
  
  def wrap_duckdb(self, idx, query):
    # Make sure the profile directory exists.
    profile_dir = self.create_dir()

    # The profile path.
    query_profile_path = os.path.join(profile_dir, f'profile-{idx}-{self.competitor}-query-hot.json')

    # And return.
    return f"""
      SET threads = {NUM_THREADS};

      -- [query] Cold run.
      {query}

      -- [query] Hot run.
      PRAGMA enable_profiling='json';
      PRAGMA profile_output='{query_profile_path}';
      {query}
    """

  def wrap_nutella(self, idx, query, helper_query=None, helper_update=None):
    # Make sure the profile directory exists.
    profile_dir = self.create_dir()

    # The profile path(s).
    helper_profile_path = os.path.join(profile_dir, f'profile-{idx}-{self.competitor}-helper-hot.json')
    query_profile_path = os.path.join(profile_dir, f'profile-{idx}-{self.competitor}-query-hot.json')
    
    assert helper_query is not None and helper_update is not None
    return f"""
      SET threads = {NUM_THREADS};
      -- [helper] Cold run
      {helper_query}

      -- [helper] Hot run.
      PRAGMA enable_profiling='json';
      PRAGMA profile_output='{helper_profile_path}';
      {helper_query}

      -- PROFILER: *OFF*.
      PRAGMA disable_profiling;
      
      -- [helper] Update.
      {helper_update}

      -- [query] Cold run.
      {query}

      -- PROFILER: *ON*.
      PRAGMA enable_profiling='json';

      -- [query] Hot run.
      PRAGMA profile_output='{query_profile_path}';
      {query}
    """

def prepare_workload(competitor: str, config_file: str, common_db_path: str, tn: str, cn: str, workload_type: str, time_limit: float, queries: List[str], partition: dict):
  print('Initializing DuckDB database..')

  # The config name.
  assert config_file.endswith('.json')
  config_name = os.path.basename(config_file).replace('.json', '')

  # Create directory.
  if competitor in ['duckdb', 'naive']:
    this_query_dir = os.path.join(QUERY_DIR, f'{tn}-{cn}', config_name, competitor, workload_type)
  else:
    assert competitor == 'optimized'
    assert time_limit is not None
    this_query_dir = os.path.join(QUERY_DIR, f'{tn}-{cn}', config_name, competitor, workload_type, f'{time_limit}')
  os.makedirs(this_query_dir, exist_ok=True)

  # Specify the database file.
  db_path = os.path.join(this_query_dir, 'temp.db')

  # Copy the database file.
  import shutil
  shutil.copyfile(common_db_path, db_path)

  byte_mapping = None
  if competitor in ['naive', 'optimized']:
    # Open the connection to build nutella.
    con = utils.open_duckdb(db_path, read_only=False, num_threads=1)

    # Register the UDF.
    fingerprint_builder = NutellaFingerprint(partition)
    con.create_function('nutella_fp', fingerprint_builder, return_type='INT32')

    # Update the nutella column.
    con.execute('''
      UPDATE tab
      SET nutella = nutella_fp(raw);
    ''')

    # Close the connection.
    con.close()

    # Take the byte mapping.
    byte_mapping = nutella.fetch_byte_mapping(None, partition)

  # Specify the wrapper.
  wrapper = QueryWrapper(competitor, tn, cn, config_name, workload_type, time_limit)

  for idx, pattern in enumerate(queries):
    wrapped_query = None
    if competitor == 'duckdb':
      # Define the nutella query.
      duckdb_query = f'''
        SELECT COUNT(*) AS match_count
        FROM tab
        WHERE raw LIKE '%' || '{utils.sql_escape(pattern)}' || '%';
      '''
      wrapped_query = wrapper.wrap_duckdb(idx, duckdb_query)
    else:
      # Build the nutella mask.
      assert byte_mapping is not None
      nutella_mask = nutella.build_fingerprint(pattern, byte_mapping)

      # Define the nutella helper.
      nutella_helper_query = f'''
        SELECT COUNT(*) as match_count
        FROM tab
        WHERE ((nutella & {nutella_mask}) = {nutella_mask});
      '''

      # Define the nutella helper.
      nutella_helper_update = f'''
        UPDATE tab
        SET helper = ((nutella & {nutella_mask}) = {nutella_mask});
      '''

      # Define the nutella query.
      nutella_query = f'''
        SELECT COUNT(*) AS match_count
        FROM tab
        WHERE helper = TRUE AND raw LIKE '%' || '{utils.sql_escape(pattern)}' || '%';
      '''

      # The wrapper query.
      wrapped_query = wrapper.wrap_nutella(idx, nutella_query, nutella_helper_query, nutella_helper_update)

    with open(os.path.join(this_query_dir, f'{competitor}-{idx}.sql'), 'w') as f:
      f.write(wrapped_query)

  print('Preparation complete: database and SQL files ready.')

def load_words(con, words, partition):  
  # Create DataFrame for words
  df = pd.DataFrame({ 'word' : words })
  
  # Build fingerprints for words if not precomputed
  byte_mapping = nutella.fetch_byte_mapping(None, partition)
  df['word_fp'] = df['word'].apply(lambda w: nutella.build_fingerprint(w, byte_mapping))

  # Register table
  con.register('words_df', df)
  con.execute('CREATE TABLE words AS SELECT * FROM words_df;')
  return con

def fetch_imdb_column_data(tn, cn):
# Fetch the column `cn` of table `tn`.
  # Load the IMDb database.
  con = utils.open_duckdb('./dbs/imdb.duckdb', read_only=True, num_threads=1)

  return con.execute(f'''
    SELECT {cn}
    FROM {tn}
    WHERE {cn} ~ '^[\\x00-\\x7F]*$';
  ''').fetchdf()[cn]

def load_table(con, tn, cn, partition):
# Loads the ASCII values of `cn` into a temporary table of `tn`.
  # Register the UDF.
  fingerprint_builder = NutellaFingerprint(partition)
  con.create_function('nutella_fp', fingerprint_builder, return_type='INT32')

  # Create the `words` table.
  con.execute(f'''
    CREATE TEMPORARY TABLE words AS
    SELECT {cn} as word, nutella_fp({cn}) AS word_fp
    FROM {tn}
    WHERE {cn} ~ '^[\\x00-\\x7F]*$';
  ''')

  # And return.
  return con

def compute_fpr(words, queries, partition, verbose=False):
  print(f'len(words)={len(words)}, len(patterns)={len(queries)}')

  # Prepare an in-memory DuckDB connection.
  con = duckdb.connect(database=':memory:')

  # Load the words.
  load_words(con, words, partition)

  # And compute the FPR.
  return nutella.run_mechanism_wrapper_with_duckdb(
    con,
    queries,
    partition,
    verbose=verbose
  )

def compute_table_fpr(tn, cn, queries, partition, verbose=False):
  print(f'tn={tn}, cn={cn} len(patterns)={len(queries)}')

  # Load the IMDb database.
  con = utils.open_duckdb('./dbs/imdb.duckdb', read_only=True, num_threads=1)

  # Load the table.
  load_table(con, tn, cn, partition)

  # And compute the FPR.
  return nutella.run_mechanism_wrapper_with_duckdb(
    con,
    queries,
    partition,
    verbose=verbose
  )

def compute_fpr_for_entry(index, num_partitions, tn, cn, timestamp, partition, solution_value, train_words, train_queries, all_words, test_queries, generalization=False, table_generalization=False, verbose=False):
  if index is not None:
    print(f'\n👷 Computing train/val/test FPRs for partition {index + 1} / {num_partitions}..')

  train_fpr = compute_fpr(train_words, train_queries, partition, verbose=verbose)
  val_fpr = compute_fpr(all_words, train_queries, partition, verbose=verbose)

  test_fpr = None
  if generalization:
    test_fpr = compute_fpr(all_words, test_queries, partition, verbose=verbose)

  table_val_fpr, table_test_fpr = None, None
  if table_generalization:
    table_val_fpr = compute_table_fpr(tn, cn, train_queries, partition, verbose=verbose)
    table_test_fpr = compute_table_fpr(tn, cn, test_queries, partition, verbose=verbose)

  return {
    'index': index,
    'timestamp': timestamp,
    'solution_value': solution_value,
    'train-fpr': train_fpr,
    'val-fpr': val_fpr,
    'test-fpr': test_fpr,
    'table-val-fpr' : table_val_fpr,
    'table-test-fpr' : table_test_fpr,
  }