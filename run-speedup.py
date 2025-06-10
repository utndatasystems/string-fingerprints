import os
import utils
import argparse
import common
import duckdb
import config_analyzer
import pandas as pd
import nutella
from concurrent.futures import ProcessPoolExecutor

CONFIG_FILE = './results/title-title/results_config_title-title_1-words-title-title-block-4-title-title-queries.json'
BLOCK_FILE = './words/title-title-block-4.txt'

NUMBER_OF_BINS = [4, 8, 16]
TIME_LIMITS = [0.1, 1, 10, 100]

config = utils.read_json(CONFIG_FILE)

def prepare_table_queries(competitor, config_file, tn, cn, partition, time_limit, train_queries, test_queries, table_generalization=False, common_db_path=None):
  assert common_db_path is not None

  # Test.
  common.prepare_workload(competitor, config_file, common_db_path, tn, cn, 'table-val', time_limit, train_queries, partition)

  # Test.
  if table_generalization:
    common.prepare_workload(competitor, config_file, common_db_path, tn, cn, 'table-test', time_limit, test_queries, partition)

def run_nutella_worker(args):
  entry, config_file, tn, cn, train_queries, test_queries, table_generalization, common_db_path = args
  threshold, _, partition, _ = entry

  prepare_table_queries(
    'optimized',
    config_file,
    tn,
    cn,
    partition,
    threshold,
    train_queries,
    test_queries,
    table_generalization=table_generalization,
    common_db_path=common_db_path
  )

# A config analyzer that runs the workload.
class RunnableConfigAnalyzer(config_analyzer.ConfigAnalyzer):
  # def analyze_duckdb(self):
  #   # Get the data.
  #   train_words, train_queries, all_words, test_queries = self.get_data()

  #   # And prep.
  #   prepare_queries(self.config_file, self.tn, self.cn, None, None, train_words, train_queries, all_words, test_queries)

  # def analyze_naive(self):
  #   # Fetch the number of bins.
  #   num_bins = self.config['configuration']['number_of_bins']

  #   # Get the data.
  #   train_words, train_queries, all_words, test_queries = self.get_data()

  #   # And prep.
  #   prepare_queries(self.config_file, self.tn, self.cn, None, None, train_words, train_queries, all_words, test_queries, num_bins=num_bins)

  def run_duckdb(self, table_generalization=False):
    # Get the data.
    train_words, train_queries, all_words, test_queries = self.get_data()

    # Fetch the column data.
    column_data = common.fetch_imdb_column_data(self.tn, self.cn)

    # Create the database.
    common_db_path = '/tmp/temp.db'
    duckdb.sql(f'INSTALL json; LOAD json;')
    con = utils.open_duckdb(common_db_path, read_only=False)
    con.execute(common.INIT_DUCKDB_SQL)

    # Register the column data.
    column_data_df = pd.DataFrame({ 'raw' : column_data })
    con.register('column_data_df', column_data_df)

    # Init the table.
    con.execute('''
      INSERT INTO tab
      SELECT raw
      FROM column_data_df;
    ''')

    # And close the connection.
    con.close()

    # And run.
    prepare_table_queries(
      'duckdb',
      self.config_file,
      self.tn,
      self.cn,
      None,
      None,
      train_queries,
      test_queries,
      table_generalization=table_generalization,
      common_db_path=common_db_path
    )

    # Delete the temporary database file (since it's been already copied to the corresponding places).
    assert os.path.exists(common_db_path)
    os.remove(common_db_path)
    return

  def run_naive(self, table_generalization=False):
    # Get the data.
    train_words, train_queries, all_words, test_queries = self.get_data()

    # Fetch the column data.
    column_data = common.fetch_imdb_column_data(self.tn, self.cn)

    # Create the database.
    common_db_path = '/tmp/temp.db'
    duckdb.sql(f'INSTALL json; LOAD json;')
    con = utils.open_duckdb(common_db_path, read_only=False)
    con.execute(common.INIT_NUTELLA_SQL)

    # Register the column data.
    column_data_df = pd.DataFrame({ 'raw' : column_data })
    con.register('column_data_df', column_data_df)

    # Init the table.
    con.execute('''
      INSERT INTO tab
      SELECT raw, 0, TRUE
      FROM column_data_df;
    ''')

    # And close the connection.
    con.close()

    # Specify the naive partition.
    num_bins = self.config['configuration']['number_of_bins']
    naive_partition = nutella.revert_byte_mapping(nutella.fetch_byte_mapping(None, None, num_bins=num_bins))

    # And run.
    prepare_table_queries(
      'naive',
      self.config_file,
      self.tn,
      self.cn,
      naive_partition,
      None,
      train_queries,
      test_queries,
      table_generalization=table_generalization,
      common_db_path=common_db_path
    )

    # Delete the temporary database file (since it's been already copied to the corresponding places).
    assert os.path.exists(common_db_path)
    os.remove(common_db_path)
    return

  def run_optimized(self, table_generalization=False):
    # Get the data.
    train_words, train_queries, all_words, test_queries = self.get_data()

    # Get the number of partitions.
    num_partitions = len(self.config.get("intermediate_solutions_time_partition", []))

    # Compute the timestamp offset.
    timestamp_offset = self.config['timing']['time_compute_data'] + self.config['timing']['time_building_model']

    # Take all the entries.
    entries = self.config.get("intermediate_solutions_time_partition", [])

    # Add the offset.
    entries = [(ts + timestamp_offset, partition, value) for ts, partition, value in entries]

    # Take the chosen ones.    
    chosen_entries = []
    for entry in entries:
      chosen_entries.append((entry[0], *entry))
    del entries

    # Fetch the column data.
    column_data = common.fetch_imdb_column_data(self.tn, self.cn)

    # Create the database.
    common_db_path = '/tmp/temp.db'
    duckdb.sql(f'INSTALL json; LOAD json;')
    con = utils.open_duckdb(common_db_path, read_only=False)
    con.execute(common.INIT_NUTELLA_SQL)

    # Register the column data.
    column_data_df = pd.DataFrame({ 'raw' : column_data })
    con.register('column_data_df', column_data_df)

    # Init the table.
    con.execute('''
      INSERT INTO tab
      SELECT raw, 0, TRUE
      FROM column_data_df;
    ''')

    # And close the connection.
    con.close()

    # Run in parallel.
    args_list = [
      (entry, self.config_file, self.tn, self.cn, train_queries, test_queries, table_generalization, common_db_path)
      for entry in chosen_entries
    ]

    with ProcessPoolExecutor() as executor:
      list(executor.map(run_nutella_worker, args_list))

    # Delete the temporary database file (since it's been already copied to the corresponding places).
    assert os.path.exists(common_db_path)
    os.remove(common_db_path)
    return

def main():
  parser = argparse.ArgumentParser(description="Prepare queries.")

  parser.add_argument('folder', type=str, help='Folder with result JSON files')
  parser.add_argument('block', type=str, help='Block number in filename')
  args = parser.parse_args()

  assert os.path.exists(args.folder)
  if args.folder.endswith('/'):
    args.folder = args.folder[:-1]
  print(os.path.basename(args.folder))
  tn, cn = os.path.basename(args.folder).split('-')

  # Matching files.
  files = utils.find_matching_config_files(args.folder, tn, cn, args.block)

  # No files?
  if not files:
    print('No matching files found.')
    return

  # Collect the data.
  configs = utils.collect_configs(files)

  # Filter the collection.
  configs = utils.filter_configs(
    configs,
    number_of_bins=NUMBER_OF_BINS,
    subset_word_selection_method_nr_words=[50],
    subset_pattern_nr_patterns=[20]
  )

  # Check each config.
  for config in configs:
    print(f'\n==== {config['file']} ====')

    # Init the analyzer.
    analyzer = RunnableConfigAnalyzer(tn, cn, config['file'])

    # Run `duckdb`.
    analyzer.run_duckdb(table_generalization=True)

    # Run naive nutella..
    analyzer.run_naive(table_generalization=True)

    # Run the optimized nutella.
    analyzer.run_optimized(table_generalization=True)

if __name__ == '__main__':
  main()