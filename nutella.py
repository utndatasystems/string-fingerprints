import utils
import statistics
import random
import math
import sys
import os
import duckdb
import pandas as pd

def read_partition(partition_file):
  optimized_partition = utils.read_json(sys.argv[4])
  if 'partition' in optimized_partition:
    return optimized_partition['partition']
  return optimized_partition

def sample_partition(bin_count=4):
  chars = list(range(256))
  random.shuffle(chars)

  partition = {i: [] for i in range(bin_count)}
  for idx, char in enumerate(chars):
    bin_idx = idx % bin_count
    partition[bin_idx].append(char)

  return partition

def revert_byte_mapping(partition):
  ret = {}
  for key in partition:
    if partition[key] not in ret:
      ret[partition[key]] = []
    ret[partition[key]].append(key)
  return ret

def fetch_byte_mapping(pattern, partition, num_bins=None):
  # Return the default one.
  if partition is None:
    assert num_bins is not None
    return {byte: byte % num_bins for byte in range(256)}

  ret = {}
  for bin_idx in partition:
    for chr in partition[bin_idx]:
      if isinstance(chr, str):
        # Put the ASCII-number in the mapping.
        ret[ord(chr)] = int(bin_idx)
      else:
        ret[chr] = int(bin_idx)
  
  return ret

def build_fingerprint(text: str, byte_mapping: dict, verbose = False) -> int:
  fingerprint = 0
  for byte_idx, byte in enumerate(text.encode('utf-8')):
    if byte not in byte_mapping:
      print(f'text={text}, byte_idx={byte_idx}, {text[byte_idx-2:byte_idx+2]}, len(utf-8)={len(text.encode('utf-8'))}')
      assert 0, f'Byte: {byte} is not in your mapping!'
    fingerprint |= 1 << byte_mapping[byte]
  return fingerprint

def compute_fingerprint_density(fingerprint):
  count = 0
  while fingerprint:
    count += 1
    fingerprint &= (fingerprint - 1)
  return count

def summarize(xs, simple=False):
  if simple:
    return {
      'avg' : sum(xs) / len(xs)
    }
  return {
    'avg' : sum(xs) / len(xs),
    'geo' : math.exp(sum(math.log(x + 1e-4) for x in xs) / len(xs)),
    'med' : statistics.median(xs)
  }

def compute_data_stats(words, optimized_partition=None):
  # Fetch the alphabet mapping.
  byte_mapping = fetch_byte_mapping(None, optimized_partition)

  # The array.
  num_of_ones = []

  for word in words:
    # Build the word fingerprint.
    word_fingerprint = build_fingerprint(word, byte_mapping)

    # Compute and
    num_of_ones.append(compute_fingerprint_density(word_fingerprint))

  # And return.
  return num_of_ones

def compute_workload_stats(workload, optimized_partition=None):
  # Fetch the alphabet mapping.
  byte_mapping = fetch_byte_mapping(None, optimized_partition)

  # The array.
  num_of_ones = []

  for query in workload:
    # Build the word fingerprint.
    query_fingerprint = build_fingerprint(query, byte_mapping)

    # Compute and
    num_of_ones.append(compute_fingerprint_density(query_fingerprint))

  # And return.
  return num_of_ones

def run_with_duckdb(con, pattern, partition=None, num_bins=None):
  # Fetch the alphabet mapping.
  byte_mapping = fetch_byte_mapping(pattern, partition, num_bins=num_bins)

  # Build the fingerprint for the pattern.
  pattern_fingerprint = build_fingerprint(pattern, byte_mapping)

  # Query for false positives (bitmask match but not actual substring match).
  num_fps = con.execute(
    '''
    SELECT COUNT(*) AS num_fps
    FROM words
    WHERE ((word_fp & ?) = ?)
      AND NOT (word LIKE '%' || ? || '%')
    ''',
    [pattern_fingerprint, pattern_fingerprint, pattern]
  ).fetchdf()['num_fps'][0]

  # Query for total negatives (all words that don’t contain the pattern).
  num_negs = con.execute(
    '''
    SELECT COUNT(*) AS num_negs
    FROM words
    WHERE NOT (word LIKE '%' || ? || '%')
    ''',
    [pattern]
  ).fetchdf()['num_negs'][0]

  return {
    '#FPs': num_fps,
    '#Ns': num_negs
  }

def run(words, pattern, partition=None, num_bins=None):
  # Fetch the alphabet mapping.
  byte_mapping = fetch_byte_mapping(pattern, partition, num_bins=num_bins)

  # Build the pattern fingerprint.
  # verbose = False
  # if pattern == ' ':
  #   verbose = True
  pattern_fingerprint = build_fingerprint(pattern, byte_mapping)#, verbose)

  # if verbose:
  #   print(f'pattern={pattern} => {pattern_fingerprint}')
  #   print(byte_mapping)

  # And run.
  num_fps, num_tns, num_exact_matches, num_nutella_matches, num_negative_matches = 0, 0, 0, 0, 0

  # TODO: Maybe take `set`?
  for word in words:
    # Build the word fingerprint.
    word_fingerprint = build_fingerprint(word, byte_mapping)

    # Check the exact match.
    is_match = (pattern in word)
    num_exact_matches += is_match

    # Check the nutella match.
    nutella_match = ((word_fingerprint & pattern_fingerprint) == pattern_fingerprint)
    num_nutella_matches += nutella_match 

    # The number of negative matches.
    num_negative_matches += (not is_match)

    # FPs.
    num_fps += (nutella_match and (not is_match))

    # TNs.
    num_tns += ((not nutella_match) and (not is_match))

  # print(f'pattern=..{pattern}.. >>>>> num_fps={num_fps}, num_tns={num_tns}')

  # Return FPR.
  assert num_negative_matches == (num_fps + num_tns)
  return {
    '#FPs' : num_fps,
    '#Ns' : num_negative_matches
  }

from concurrent.futures import ThreadPoolExecutor, as_completed

def process_pattern(words, pattern, optimized_partition, run_default):
  # Run the default case
  default_ret = run(words, pattern) if run_default else None

  # Run the optimized case
  optimized_ret = run(words, pattern, partition=optimized_partition)

  return {
    "pattern": pattern,
    "default_fpr": default_ret['FPR'] if run_default else None,
    "optimized_fpr": optimized_ret['FPR']
  }

# def run_workload_parallel(words, workload, optimized_partition, run_default=True):
#   max_len = max(map(len, workload))
#   results = []

#   with ThreadPoolExecutor() as executor:
#     futures = [
#       executor.submit(process_pattern, words, pattern, optimized_partition, run_default)
#       for pattern in workload
#     ]

#     for future in as_completed(futures):
#       result = future.result()
#       results.append(result)
#       if run_default:
#         pattern = result['pattern']
#         default_fpr = result['default_fpr']
#         optimized_fpr = result['optimized_fpr']
#         print(f'{pattern}{(max_len - len(pattern)) * " "} => FPR [naive / optimized]: {default_fpr * 100:.2f}% vs. {optimized_fpr * 100:.2f}%')

#   default_fprs = [r['default_fpr'] for r in results] if run_default else []
#   optimized_fprs = [r['optimized_fpr'] for r in results]

#   return default_fprs, optimized_fprs

def agg_info(info):
  num_fps = sum(x['#FPs'] for x in info)
  num_negs = sum(x['#Ns'] for x in info)
  fpr = num_fps / num_negs if num_negs > 0 else math.inf
  return num_fps, num_negs, fpr

def run_mechanism_wrapper_with_duckdb(con, workload, partition, verbose=False):
  # Run the experiment.
  info = []
  for pattern in workload:
    ret = run_with_duckdb(con, pattern, partition=partition)
    info.append({
      '#FPs': ret['#FPs'],
      '#Ns': ret['#Ns']
    })

  # Aggregate.
  num_fps, num_negs, fpr = agg_info(info)

  # Optional logging
  if verbose:
    print(f'# [|workload|={len(workload)}] => #TNS={num_negs - num_fps}, #FPs={num_fps}, #Ns={num_negs}, FPR={fpr}')

  return fpr

def run_mechanism(words, workload, partition, verbose=False):
  # Take the max. length.
  max_len = max(map(len, workload))

  info = []
  for pattern in workload:
    # Run the optimized case.
    ret = run(words, pattern, partition=partition)

    # Append the FPR.
    info.append({
      '#FPs' : ret['#FPs'],
      '#Ns' : ret['#Ns']
    })

  # Aggregate.
  num_fps, num_negs, fpr = agg_info(info)

  if verbose:
    print(f'# [|words|={len(words)} |workload|={len(workload)}] => #TNS={num_negs - num_fps}, #FPs={num_fps}, #Ns={num_negs}, FPR={fpr}')

  return fpr

# def run_workload(words, workload, optimized_partition, run_default=True):
#   # Take the max. length.
#   max_len = max(map(len, workload))

#   # FPR lists
#   default_fprs, optimized_fprs = [], []

#   num_bins = None
#   if run_default:
#     # Get the number of bins.
#     num_bins = len(optimized_partition.keys())

#   for pattern in workload:
#     # Run the default case.
#     if run_default:
#       default_ret = run(words, pattern, num_bins=num_bins)
#       default_fprs.append(default_ret['FPR'])

#     # Run the optimized case.
#     optimized_ret = run(words, pattern, partition=optimized_partition)
#     optimized_fprs.append(optimized_ret['FPR'])

#     if run_default:
#       print(f'{pattern}{(max_len - len(pattern)) * " "} => FPR [naive / optimized]: {default_ret["FPR"] * 100:.2f}% vs. {optimized_ret["FPR"] * 100:.2f}%')

#   return default_fprs, optimized_fprs

# def run_workload_wrapper(words_file, workload_file, obj_file):
#   # Read the words.
#   words = [line.strip('\n') for line in open(words_file)]

#   # Read the workload.
#   workload = [line.strip('\n') for line in open(workload_file)]

#   # Read the partition from the config / partition file.
#   obj = utils.read_json(obj_file)
#   if 'partition' in obj:
#     optimized_partition = obj['partition']
#   else:
#     optimized_partition = obj
    
#   # And run.
#   return run_workload_parallel(words, workload, optimized_partition)

def test_convergence_behavior(words, workload):
  # And run.
  print(f'Running..')

  best_stats = None
  for iter in range(100):
    curr_fprs = []

    # Get another optimized partition.
    partition = sample_partition()

    # Get the data stats.
    data_stats = summarize(compute_data_stats(words, partition))

    for pattern in workload:
      # Run the optimized case.
      ret = run(words, pattern, partition=partition)

      # Store the FPRs.
      curr_fprs.append(ret['FPR'])

    # Compute summary.
    curr_stats = summarize(curr_fprs)
    
    # Yield only if the current stats are better
    if best_stats is None or curr_stats['avg'] < best_stats['avg']:
      best_stats = curr_stats

      best_stats['#ones'] = data_stats['avg']

      print(f'====\nIteration {iter}:')
      print(f'  #ones            : {data_stats['avg']:.2f}')
      print(f'  New best avg. FPR: {best_stats["avg"] * 100:.2f}%')
      print(f'  New best geo. FPR: {best_stats["geo"] * 100:.2f}%')
      print(f'  New best med. FPR: {best_stats["med"] * 100:.2f}%')

    yield {
      'iteration': iter,
      '#ones' : best_stats['#ones'],
      'avg'   : best_stats['avg'] * 100,
      'geo'   : best_stats['geo'] * 100,
      'med'   : best_stats['med'] * 100
    }

def plot(generator):
  import matplotlib.pyplot as plt

  plt.ion()
  fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 10), sharex=True)

  iterations = []
  best_num_ones, best_avg_fprs, best_geo_fprs, best_med_fprs = [], [], [], []

  for data in generator:
    iterations.append(data['iteration'])
    best_num_ones.append(data['#ones'])
    best_avg_fprs.append(data['avg'])
    best_geo_fprs.append(data['geo'])
    best_med_fprs.append(data['med'])

    # Clear previous plots
    ax1.clear()
    ax2.clear()

    # Plot FPRs
    ax1.plot(iterations, best_avg_fprs, marker='o', label='Avg FPR')
    ax1.plot(iterations, best_geo_fprs, marker='s', label='Geo FPR')
    ax1.plot(iterations, best_med_fprs, marker='^', label='Med FPR')
    ax1.set_title('Convergence of FPR')
    ax1.set_ylabel('FPR (%)')
    ax1.grid(True, linestyle='--', alpha=0.6)
    ax1.legend()

    # Plot #ones
    ax2.plot(iterations, best_num_ones, marker='+', color='tab:orange', label='#ones')
    ax2.set_title('Convergence of #Ones')
    ax2.set_xlabel('Iteration')
    ax2.set_ylabel('#Ones')
    ax2.grid(True, linestyle='--', alpha=0.6)
    ax2.legend()

    plt.pause(0.1)

  plt.ioff()
  plt.tight_layout()
  plt.show()
