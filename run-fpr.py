import os
import utils 
import common
import config_analyzer

TABLE_NAME = 'title'
COLUMN_NAME = 'title'
BLOCK = 0
RESULTS_FOLDER = f'./results/{TABLE_NAME}-{COLUMN_NAME}'
CACHE_FOLDER = f'./cache/{TABLE_NAME}-{COLUMN_NAME}'
FORCE_RERUN = True
TIMELIMIT = None
VERBOSE = True

# Generalization flags.
GENERALIZATION = True
TABLE_GENERALIZATION = True

# CHOSEN_TIMESTAMPS = [0.1, 0.5, 1.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0]
# CHOOSE = False

os.makedirs(CACHE_FOLDER, exist_ok=True)

files = utils.find_matching_config_files(RESULTS_FOLDER, TABLE_NAME, COLUMN_NAME, BLOCK)

# files = ['./results/title-title/results_config_title-title_272-words-title-title-block-0-title-title-queries.json']

# Collect the data.
configs = utils.collect_configs(files)

# Filter the collection.
configs = utils.filter_configs(
  configs,
  number_of_bins=[4, 8, 16],
  subset_word_selection_method_nr_words=[50],
  subset_pattern_nr_patterns=[20]
)

from concurrent.futures import ProcessPoolExecutor, as_completed
import nutella

def compute_naive(config_path, generalization=False, table_generalization=False, summary_type='avg'):
  # Init the analyzer.
  analyzer = config_analyzer.ConfigAnalyzer(TABLE_NAME, COLUMN_NAME, config_path)

  # Get the data.
  train_words, train_queries, all_words, test_queries = analyzer.get_data()

  # Get the number of bins.
  num_bins = analyzer.config['configuration']['number_of_bins']

  # Get the naive partition.
  partition = nutella.revert_byte_mapping(nutella.fetch_byte_mapping(None, None, num_bins=num_bins))

  return common.compute_fpr_for_entry(
    None,
    None,
    TABLE_NAME,
    COLUMN_NAME,
    None,
    partition,
    None,
    train_words,
    train_queries,
    all_words,
    test_queries,
    generalization=generalization,
    table_generalization=table_generalization
  )

def compute_optimized(config_path, timelimit=None, generalization=False, table_generalization=False, summary_type='avg'):
  print(f'Extracting data from {config_path}')

  # Init the analyzer.
  analyzer = config_analyzer.ConfigAnalyzer(TABLE_NAME, COLUMN_NAME, config_path)

  # Get the data.
  train_words, train_queries, all_words, test_queries = analyzer.get_data()

  # Get the number of partitions.
  num_partitions = len(analyzer.config.get("intermediate_solutions_time_partition", []))

  # Compute the timestamp offset.
  timestamp_offset = analyzer.config['timing']['time_compute_data'] + analyzer.config['timing']['time_building_model']

  # Take all the entries.
  entries = analyzer.config.get("intermediate_solutions_time_partition", [])

  # Add the offset.
  entries = [(ts + timestamp_offset, partition, value) for ts, partition, value in entries]

  # if not CHOOSE:
  chosen_entries = []
  for entry in entries:
    chosen_entries.append((entry[0], *entry))
  del entries
  # else:
  #   entry_index, chosen_entries = 0, []
  #   for chosen_ts in CHOSEN_TIMESTAMPS:
  #     best_entry = None

  #     # Iterate from last used entry forward
  #     while entry_index < len(entries):
  #       ts, partition, value = entries[entry_index]
  #       if ts <= chosen_ts:
  #         best_entry = (ts, partition, value)
  #         entry_index += 1  # move forward to find a later candidate
  #       else:
  #         break  # current ts is already beyond the chosen_ts

  #     if best_entry is not None:
  #       chosen_entries.append((chosen_ts, *best_entry))

  print(f'CHOSEN entries=')
  for entry in chosen_entries:
    print(entry[0], entry[1])

  results = []
  with ProcessPoolExecutor() as executor:
    futures = []
    for index, entry in enumerate(chosen_entries):
      assert len(entry) == 4

      # Get the entry. NOTE: The offset has already been added.
      threshold, _, partition, solution_value = entry

      # Beyond timelimit? Then stop.
      if timelimit is not None and threshold > timelimit:
        break

      futures.append(
        executor.submit(
          common.compute_fpr_for_entry,
          index,
          num_partitions,
          TABLE_NAME,
          COLUMN_NAME,
          threshold,
          partition,
          solution_value,
          train_words,
          train_queries,
          all_words,
          test_queries,
          generalization=generalization,
          table_generalization=table_generalization,
          verbose=VERBOSE
        )
      )

    for future in as_completed(futures):
      result = future.result()
      results.append(result)

  # Sort results to maintain original time order
  results.sort(key=lambda x: x['index'])

  print(f'results:')
  print(results)

  return results

def run(configs, timelimit=None, generalization=False, table_generalization=False):
  for config in configs:
    # Extract the data.
    optimized_ret = compute_optimized(
      config['file'],
      timelimit=timelimit,
      generalization=generalization,
      table_generalization=table_generalization,
      summary_type='avg',
    )

    # Compute the naive FPRs.
    naive_ret = compute_naive(
      config['file'],
      generalization=generalization,
      table_generalization=table_generalization,
      summary_type='avg',
    )

    print(f'Naive')
    print(naive_ret)

    # Set the times and FPRs.
    config['plot'] = {
      'optimized' : optimized_ret,
      'naive' : naive_ret,
    }

    utils.write_json(os.path.join(CACHE_FOLDER, os.path.basename(config['file'])), config)

if __name__ == '__main__':
  if FORCE_RERUN:
    run(configs, timelimit=TIMELIMIT, generalization=GENERALIZATION, table_generalization=TABLE_GENERALIZATION)
  else:
    # Run the new ones only.
    config_todos = []
    for config in configs:
      if not os.path.exists(os.path.join(CACHE_FOLDER, os.path.basename(config['file']))):
        config_todos.append(config)
        
    run(config_todos, timelimit=TIMELIMIT, generalization=GENERALIZATION, table_generalization=TABLE_GENERALIZATION)
