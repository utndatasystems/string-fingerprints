import string
from pathlib import Path
import json
import argparse
import os
import time
import logging
from pprint import pformat
from logger_config import setup_logger, log_section
from utils.optimizationDataProvider import OptimizationDataProvider
from utils.gurobiModelBuilder import GurobiModelBuilder
from utils.solveGurobimodel import SolveGurobiModel


def generate_instance_name(config, config_filename, suffix):
    file_path_words = Path(config["file_path_words"])
    parts_words = f"{file_path_words.parent.name}-{file_path_words.stem}"
    file_path_patterns = Path(config["file_path_patterns"])
    parts_pattern = f"{file_path_patterns.stem}"

    name = (
        config_filename.replace(".json", "") + "-" + parts_words
        + "-" + parts_pattern
    )

    return name + suffix


def load_config_and_setup(args):
    with open(args.config) as f:
        config = json.load(f)

    config_filename = os.path.basename(args.config)

    os.makedirs(config["store_dir_results"], exist_ok=True)
    os.makedirs(config["store_dir_logfiles"], exist_ok=True)

    logfilename = os.path.join(
        config["store_dir_logfiles"],
        generate_instance_name(config, config_filename, ".log"),
    )

    if os.path.exists(logfilename):
        os.remove(logfilename)
        log_section("Warning: Old Logfile Is Overwritten!")

    setup_logger(logfilename)
    logger = logging.getLogger(__name__)

    return config, config_filename, logfilename, logger


def compute_data_for_optimization(config):
    log_section("Compute Data For Optimization")

    if config["alphabet_option"] == "string.printable":
        alphabet = string.printable
    else:
        raise ValueError(
            "Unsupported alphabet option " + str(config["alphabet_option"])
        )

    data_provider = OptimizationDataProvider(config, alphabet)
    data_provider.parse_words_patterns()

    # select subset of patterns
    method_patterns = config["subset_pattern_selection_method"]

    if method_patterns == "subset_patterns_shuffled_block":
        # get seed
        seed = config["subset_pattern_selection_method_seed"]
        nr_block = config["subset_pattern_nr_block"]
        max_nr_patterns = config["subset_pattern_nr_patterns"]
        data_provider.subset_patterns_shuffled_block(seed, max_nr_patterns,
                                                     nr_block)
    elif method_patterns is not None:
        # None means we do not change the given patterns
        raise ValueError(
            f"Unsupported subset_pattern_selection_method {method_patterns}"
        )

    # select subset of words
    method = config["subset_word_selection_method"]
    method_nr_words = config["subset_word_selection_method_nr_words"]

    if method == "subset_words_shuffled_block":
        seed = config["subset_word_selection_method_seed"]
        nr_block = config["subset_word_nr_block"]
        data_provider.subset_words_shuffled_block(seed, method_nr_words,
                                                  nr_block)

    elif method is not None:
        raise ValueError(f"Unsupported subset_word_selection_method {method}")

    data_provider.determine_pattern_words_relation()
    data_provider.index_set_pattern_letters()
    data_provider.index_set_word_letters()

    return data_provider, alphabet


def build_optimization_model(config, data_provider):
    log_section("Build Optimization Model")
    model_builder = GurobiModelBuilder(data_provider, config)
    model_builder.build_model()

    return model_builder


def solve_model(config, model_builder, data_provider):
    log_section("Solve Gurobi Model")
    solver_model = SolveGurobiModel(model_builder, data_provider, config)
    solver_model.solve()
    solver_model.get_solution_and_optimization_status()
    solver_model.get_bucket_partition()
    solver_model.get_runtime()
    return solver_model


def store_results(config, config_filename, solver_model, overall_results):
    """
    Write results dictionary to json file
    """
    file_name = "results_" + generate_instance_name(config,
                                                    config_filename, ".json")
    file_path_results = os.path.join(config["store_dir_results"], file_name)
    solver_model.write_dict_to_json(file_path_results, overall_results)


def get_partitions_intermediate_solutions(overall_results, logger, timing):
    """
    Compute for each intermediate solution the corresponding partition
    Return list of (time_point, partition, objective_value)
    list_time_point_partitions_intermediate_sols = []
    """
    list_time_point_partitions_intermediate_sols = []

    # check that last intermediate partition has an optimal objective value
    if (
        overall_results["intermediate_solutions_time_vars"][-1][2]
        != overall_results["optimal_objective_value"]
    ):
        logger.info(
            "Best known solution not in Intermediate Solutions, e.g., can happen for specific heuristics..."
        )
        logger.info(
            "Manually add last solution to intermediate with time step total gurobi solution time"
        )
        overall_results["intermediate_solutions_time_vars"].append(
            (
                timing["gurobi_solution_time"],
                overall_results["gurobi_solution"],
                overall_results["optimal_objective_value"],
            )
        )

    assert (
        overall_results["intermediate_solutions_time_vars"][-1][2]
        == overall_results["optimal_objective_value"]
    )

    # compute partitions for intermediate solutions
    for item in overall_results["intermediate_solutions_time_vars"]:
        # triples are converted to lists when writing to json files (time_point, intermediate_sol, obj_value)
        # obj_bound is current best bound at this point in the solving process
        assert len(item) == 3
        time_point = item[0]
        solution = item[1]
        obj_val = item[2]
        #  number of bins
        number_bins = int(overall_results["configuration"]["number_of_bins"])
        assert number_bins >= 0

        # take all x values that are one by variable name "x["
        x_solution = {
            var_name: value
            for var_name, value in solution.items()
            if ("x[" == var_name[:2] and 1 - 1e-6 <= value <= 1 + 1e-6)
        }

        if overall_results["configuration"]["alphabet_option"] == "string.printable":
            assert len(x_solution) == len(string.printable)
        else:
            raise ValueError(
                "Unsupported alphabet "
                + str(overall_results["configuration"]["alphabet_option"])
            )

        # compute partition
        partition = {i: [] for i in range(number_bins)}
        for var_name in x_solution:
            # varname is x[letter_pos, bin_pos]
            # remove x[ and ]
            inner = var_name[2:-1]
            letter_pos, bin_pos = map(int, inner.split(","))
            partition[bin_pos].append(overall_results["alphabet"][letter_pos])

        # create partition
        list_time_point_partitions_intermediate_sols.append(
            (time_point, partition, obj_val)
        )

    return list_time_point_partitions_intermediate_sols


def main():
    start_time = time.perf_counter()
    timing = {}
    overall_results = {}

    parser = argparse.ArgumentParser(description="Run String Fingerprint Optimization")
    parser.add_argument(
        "-config", type=str, required=True, help="Path to the config JSON file"
    )
    args = parser.parse_args()

    config, config_filename, logfilename, logger = load_config_and_setup(args)

    overall_results["configuration"] = config
    log_section("Instance Parameters")
    logger.info(pformat(config))

    # === Compute Data ===
    data_start = time.perf_counter()
    data_provider, alphabet = compute_data_for_optimization(config)
    overall_results["words_in_optimization"] = data_provider.words
    overall_results["patterns_in_optimization"] = data_provider.patterns
    overall_results["alphabet"] = alphabet
    timing["time_compute_data"] = time.perf_counter() - data_start

    # === Build Model ===
    model_start = time.perf_counter()
    model_builder = build_optimization_model(config, data_provider)
    timing["time_building_model"] = time.perf_counter() - model_start

    # === Solve Model ===
    solve_start = time.perf_counter()
    solver_model = solve_model(config, model_builder, data_provider)
    timing["gurobi_solution_time"] = solver_model.runtime
    timing["time_solving_model"] = time.perf_counter() - solve_start
    overall_results["gurobi_numeric_status"] = solver_model.status_code
    overall_results["gurobi_status"] = solver_model.optimization_status
    overall_results["intermediate_solutions_time_vars"] = (
        solver_model.intermediate_solutions
    )
    overall_results.update(
        {
            "gurobi_solution": solver_model.sparse_sol,
            "partition": solver_model.dict_bucket_items,
            "optimal_objective_value": solver_model.optimal_objective_value,
            "final_optimality_gap": solver_model.final_gap,
            "time_point_best_current_bound": solver_model.time_best_current_bound,
        }
    )

    # compute for all intermediate solutions the corresponding partition and store it
    overall_results["intermediate_solutions_time_partition"] = (
        get_partitions_intermediate_solutions(overall_results, logger, timing)
    )

    # === Finalize ===
    log_section("Total Time")
    timing["total_time"] = time.perf_counter() - start_time
    logger.info(f"Total time: {timing['total_time']:.4f} seconds")
    overall_results["timing"] = timing
    # store results
    store_results(config, config_filename, solver_model, overall_results)


if __name__ == "__main__":
    main()
