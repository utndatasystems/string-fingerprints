# String Fingerprint Optimization

This repository contains the code for running bucket-based partitioning optimizations using Gurobi, as described in the accompanying research paper.

---

## 1. Setup Instructions

### Create and Activate the Conda Environment

# linux users
```bash
conda env create -f environment.yml
conda activate string-fingerprints
```
# mac users
```bash
conda env create -f environment_mac.yml
conda activate string-fingerprints
```

### 2. Gurobi Academic License

This project uses the Gurobi Optimizer for solving integer programming problems.

How to get a free academic license:

1. Visit the [Gurobi Academic Program page](https://www.gurobi.com/academia/academic-program-and-licenses) to request your license.

2. Register with a verified academic email.

3. Follow the instructions to activate your license using grbgetkey.

### 3. Data Overview

- Queries: Located in `./data/queries/hyper-job/`
- Words: Located in `./data/words/`

### 4. Instance Configurations

In `./instance-configurations/` you can find specific instance configurations for the query data combinations.

For example, in the folder `company_name-name` you can find all instances for the queries `company_name-name-queries.txt` (see `./data/queries/hyper-job/`) and the data `company_name-name-block-xxx.txt` (see `./data/words/`).

An instance file, e.g., `config_company_name-name_0.json`, has the following structure:

```json
{
  # Used characters for partitioning from the string.printable python package
  "alphabet_option": "string.printable",
  # depreciated config option
  "bool_balance_bins": false,
  # equivalently linearize optimization as mentioned in the paper
  "bool_linearize": true,
  "file_path_patterns": "data/queries/hyper-job/company_name-name-queries.txt",
  "file_path_words": "data/words/company_name-name-block-0.txt",
  # depreciated config option
  "fix_partition": false,
  "gurobi_parameter": {
    "threads": 4,
    "timelimit": 36000.0
  },
  "number_of_bins": 4,
  "store_dir_results": "./results/company_name-name",
  "store_dir_logfiles": "./logfiles/company_name-name",
  "store_intermediate_solutions": true,
  "subset_pattern_selection_method": "subset_patterns_shuffled_block",
  # depreciated config option
  "subset_pattern_selection_method_percentage": null,
  # parameters for selecting randomly the words
  "subset_pattern_selection_method_seed": 0,
  "subset_pattern_nr_block": 0,
  "subset_pattern_nr_patterns": 4,
  "subset_word_selection_method": "subset_words_shuffled_block",
  "subset_word_selection_method_nr_words": 25,
  "subset_word_selection_method_seed": 0,
  "subset_word_nr_block": 0,
  # depreciated config option
  "presolve_set_of_patterns_words": false,
  # depreciated config option
  "max_diff_bins": null
}
```

### 5. Run Instances

```bash
python run_string_fingerprint_optimization.py -config instance-configurations/name-name/config_name-name_0.json
```

The results will be stored in a JSON file in `store_dir_results` with the name `results_config_company_name-name_0-company_name-name-block-0-company_name-name-queries.json`.

A logfile will also be stored in `store_dir_logfiles` with the name `config_company_name-name_0-company_name-name-block-0-company_name-name-queries.log`.
