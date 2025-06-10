
import utils

class ConfigAnalyzer:
  def __init__(self, tn, cn, config_file):
    self.tn, self.cn = tn, cn
    self.config_file = config_file
    self.config = utils.read_json(config_file)

  def get_sol_under_timelimit(self, time_limit):
    inter_sols = self.config['intermediate_solutions_time_partition']
    if inter_sols[0][0] > time_limit:
      return None
    for index, (t, _, _) in enumerate(inter_sols):
      if t > time_limit:
        return inter_sols[index - 1]
    return inter_sols[-1]

  def get_data(self):
    # Train words.
    train_words = self.config['words_in_optimization']

    # Train queries.
    train_queries = self.config['patterns_in_optimization']

    # Full block.
    all_words = utils.read_words(self.config['configuration']['file_path_words'].replace('data/', ''))

    # All queries.
    all_queries = utils.read_words(self.config['configuration']['file_path_patterns'].replace('data/', ''), strip_spaces=False)

    print(f'all_queries={all_queries}')
    print(f'train_qeuries={train_queries}')

    # Test queries.
    test_queries = utils.subtract_preserve_order(all_queries, train_queries)

    return train_words, train_queries, all_words, test_queries