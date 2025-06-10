import os
import re
import json
import duckdb

def sql_escape(s: str) -> str:
  return s.replace("'", "''")

def read_words(file_path, strip_spaces=False):
  assert os.path.isfile(file_path), f'File {file_path} does not exist!'
  if strip_spaces:
    return [line.strip() for line in open(file_path)]
  return [line.strip('\n') for line in open(file_path)]

def load_imdb_string_cols(file_path):
  all_patterns = read_json(file_path)

  ret = set()
  for config in all_patterns:
    ret.add((config[0], config[1]))
  return ret

def load_imdb_queries(file_path, table_name, col_name, unique=True):
  all_patterns = read_json(file_path)

  ret = []
  for config in all_patterns:
    if config[0] == table_name and config[1] == col_name:
      # Get the LIKE-expression.
      like_expr = config[4]

      # And strip the '%'-symbol.
      ret.append(like_expr.replace('%', ''))

  if not unique:
    return ret
  return list(set(ret))

def read_json(json_path):
  # Check if the file exists.
  assert os.path.isfile(json_path)

  # And read the data.
  f = open(json_path, 'r', encoding='utf-8')
  data = json.load(f)
  f.close()

  # Return it.
  return data

def write_json(json_path, json_content, format=True):
  f = open(json_path, 'w', encoding='utf-8')

  # Plain write.
  if not format:
    f.write(json_content)
  else:
    # Dump nicely.
    json.dump(json_content, f, indent=2, ensure_ascii=False)
  f.close()

def open_duckdb(db_path, read_only, num_threads=None, parachute_stats_file='', profile_output=None):
  con = duckdb.connect(db_path, read_only=read_only)

  if num_threads is not None:
    con.sql(f'SET threads to {num_threads};')
    # con.sql(f'set timer = on;')
    tmp = con.sql("SELECT current_setting('threads') as thread_count;").df()['thread_count'].values[0]
    assert tmp == num_threads

  # Set the profiler, if any.
  if profile_output is not None:
    con.sql(f"PRAGMA enable_profiling='json';")
    con.sql(f"PRAGMA profile_output='{profile_output}'")

  return con

from collections import Counter
from typing import List

def subtract_preserve_order(full_list: List[str], to_remove: List[str]) -> List[str]:
# Subtracts `to_remove` from `full_list` while preserving order and respecting duplicates.

  to_remove_counter = Counter(to_remove)
  full_counter = Counter(full_list)

  # print(to_remove_counter)
  # print(full_counter)

  # for key in to_remove_counter:
  #   print(f'key=||{key}|| => {to_remove_counter[key]} vs. {full_counter[key]}')

  assert all((to_remove_counter[q] <= full_counter[q] for q in to_remove_counter)), "Not a multi-set!"

  result = []
  for item in full_list:
    if to_remove_counter[item] > 0:
      to_remove_counter[item] -= 1
    else:
      result.append(item)
  
  return result

def find_matching_config_files(folder, table, column, block):
  pattern = re.compile(
    rf"results_config_{re.escape(table)}-{re.escape(column)}_\d+-words-{re.escape(table)}-{re.escape(column)}-block-{re.escape(f'{block}')}-.*?-queries\.json"
  )

  ret = []
  for f in os.listdir(folder):
    if pattern.match(f):
      ret.append(os.path.join(folder, f))
  return ret

def collect_configs(file_paths, plot_balance=True, plot_seed=False):
  ret = []
  for file_path in file_paths:
    data = read_json(file_path)
      
    # is_balanced = data['configuration']['bool_balance_bins']
    train_data_size = data['configuration']['subset_word_selection_method_nr_words']
    number_of_bins = data['configuration']['number_of_bins']
    train_query_size = data['configuration']['subset_pattern_nr_patterns']
    subset_pattern_nr_block = data['configuration']['subset_pattern_nr_block']

    assert train_query_size in [20, 40]

    # Take only the first run.
    if subset_pattern_nr_block != 0:
      continue

    ret.append({
      'file' : file_path,
      'number_of_bins' : number_of_bins,
      'subset_word_selection_method_nr_words' : train_data_size,
      'subset_pattern_nr_patterns' : train_query_size
    })
  return ret

def filter_configs(collection, number_of_bins: List[int], subset_word_selection_method_nr_words: List[int], subset_pattern_nr_patterns: List[int]):
  return list(
    filter(
      lambda elem:
        elem['number_of_bins'] in number_of_bins and
        elem['subset_word_selection_method_nr_words'] in subset_word_selection_method_nr_words and
        elem['subset_pattern_nr_patterns'] in subset_pattern_nr_patterns,
      collection
    )
  )