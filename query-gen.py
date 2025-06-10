import os
import utils
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed

QUERY_FILE = 'like-in-imdb.json'
WORDS_DIR = 'words'
QUERIES_DIR = 'queries/meta-job'
N_GRAM_MAX = 10
TOP_K = 10

os.makedirs(QUERIES_DIR, exist_ok=True)

def read_block_lines(tn, cn, i):
  path = f'{WORDS_DIR}/{tn}-{cn}-block-{i}.txt'
  if not os.path.exists(path):
    return []
  with open(path, 'r', encoding='utf-8') as f:
    return [line.strip() for line in f if line.strip()]

def get_all_block_ids(tn, cn):
  i = 0
  ids = []
  while os.path.exists(f'{WORDS_DIR}/{tn}-{cn}-block-{i}.txt'):
    ids.append(i)
    i += 1
  return ids

def compute_ngrams_from_block(args):
  tn, cn, blk_id = args
  lines = read_block_lines(tn, cn, blk_id)
  local_counters = {n: Counter() for n in range(1, N_GRAM_MAX + 1)}
  for text in lines:
    length = len(text)
    for i in range(length):
      for n in range(1, N_GRAM_MAX + 1):
        if i + n <= length:
          ngram = text[i:i+n]
          local_counters[n][ngram] += 1
  return local_counters

def merge_counters(global_counters, local_counters):
  for n in range(1, N_GRAM_MAX + 1):
    global_counters[n].update(local_counters[n])

def debug(n, vs, msg):
  print(f'==== {n}::{msg} ====')
  print(vs)

def get_top_bands(n, counter, top_k):
  items = counter.most_common()
  total = len(items)
  if total == 0:
    return []

  one_third = total // 3
  high_band = items[:one_third]
  mid_band = items[one_third:2*one_third]
  low_band = items[2*one_third:]

  high = [item[0] for item in high_band[:top_k]]
  mid = [item[0] for item in mid_band[:top_k]]
  low = [item[0] for item in low_band[:top_k]]

  debug(n, high, 'high')
  debug(n, mid, 'mid')
  debug(n, low, 'low')
  return high + mid + low

if __name__ == '__main__':
  for tn, cn in utils.load_imdb_string_cols(QUERY_FILE):
    print(f"Processing {tn}.{cn}...")

    ngram_counters = {n: Counter() for n in range(1, N_GRAM_MAX + 1)}
    block_ids = get_all_block_ids(tn, cn)
    tasks = [(tn, cn, blk_id) for blk_id in block_ids]

    with ProcessPoolExecutor() as executor:
      futures = [executor.submit(compute_ngrams_from_block, task) for task in tasks]

      for future in as_completed(futures):
        result = future.result()
        merge_counters(ngram_counters, result)

    total_count = sum(len(ngram_counters[n]) for n in range(1, N_GRAM_MAX + 1))
    if not total_count:
      continue

    queries = []
    for n in range(1, N_GRAM_MAX + 1):
      top_patterns = get_top_bands(n, ngram_counters[n], TOP_K)
      queries.extend(top_patterns)

    queries = list(filter(lambda x: x.strip(), queries))

    with open(f"{QUERIES_DIR}/{tn}-{cn}-queries.txt", 'w', encoding='utf-8') as f:
      f.write('\n'.join(queries))

  print("✅ Parallelized query generation complete.")