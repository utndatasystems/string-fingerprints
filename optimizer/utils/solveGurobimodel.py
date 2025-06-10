import gurobipy as gp
import json
import io
from contextlib import redirect_stdout
import logging


class SolveGurobiModel:
    def __init__(self, gurobi_model_builder, data_provider, config):
        self._gurobi_model_builder = gurobi_model_builder
        self._model = gurobi_model_builder._model
        self._data_provider = data_provider  # containing all instances data
        self._config = config
        self.sparse_sol = {}  # solution without variables that are zero
        self._feasible_point_exists = False
        self.status_code = None  # numeric gurobi status
        self.optimization_status = None  # optimization status gurobi
        self.dict_bucket_items = {}  # dictionary partitioning of alphabet into buckets
        # list (time, solution) for all solutions found in solution process
        self.intermediate_solutions = []
        # get logger from main file
        # tuples time_point, best_current_bound
        self.time_best_current_bound = []
        # stores values of current best incumbunt: max lower bound -inf
        self.best_obj_val = -float("inf")
        # maximization problem -> will be overwritten in first step
        self._best_current_bound = float("inf")
        # runtime of solution process
        self.runtime = None
        self.optimal_objective_value = None
        self.final_gap = None

    def solve(self):
        """
        Solve the model
        """
        logger = logging.getLogger("gurobi")
        log_buffer = io.StringIO()

        with redirect_stdout(log_buffer):
            self._model.setParam("Threads", self._config["gurobi_parameter"]["threads"])
            self._model.setParam(
                "TimeLimit", self._config["gurobi_parameter"]["timelimit"]
            )

            if self._config["store_intermediate_solutions"]:
                self._model.optimize(self.my_callback)
            else:
                self._model.optimize()

    def get_solution_and_optimization_status(self):
        """
        Store Gurobi solution in self.sparse_sol.
        Only stores variables with non-zero value.
        Format: {var_name: value}
        """
        self.sparse_sol = {}
        self.optimal_objective_value = None
        self.status_code = self._model.Status
        self.optimization_status = self.gurobi_status_code_mapping()

        # get logger
        logger = logging.getLogger("run_string_fingerprint_optimization")
        logger.info(f"Solver status: {self.optimization_status}")

        # List of Gurobi statuses that may provide a feasible solution
        feasible_statuses = {
            gp.GRB.OPTIMAL,
            gp.GRB.SUBOPTIMAL,
            gp.GRB.TIME_LIMIT,
            gp.GRB.SOLUTION_LIMIT,
            gp.GRB.USER_OBJ_LIMIT,
            gp.GRB.WORK_LIMIT,
        }

        if self.status_code in feasible_statuses:
            if self._model.SolCount > 0:
                self._feasible_point_exists = True
                for var in self._model.getVars():
                    if abs(var.X) > 1e-6:  # Avoid floating-point noise
                        self.sparse_sol[var.VarName] = var.X
                self.optimal_objective_value = self._model.ObjVal
                self.final_gap = self._model.MIPGap
            else:
                logger.info(
                    "⚠️ Feasible status reported but no solution found (SolCount == 0)."
                )
        else:
            logger.info(
                f"❌ No feasible point available. Gurobi status code: {self.status_code}"
            )

    def get_bucket_partition(self):
        """
        Get from solution the bucket partition
        """
        # skip if we have no feasible point
        if not self._feasible_point_exists:
            return

        # initialize buckets
        self.dict_bucket_items = {
            bucket: [] for bucket in range(0, self._data_provider.number_bins)
        }

        x_solution = {
            index_tuple: var.X
            for index_tuple, var in self._gurobi_model_builder.x_vars.items()
            if abs(var.X) > 1e-6
        }

        for letter_pos, bucket in x_solution.keys():
            self.dict_bucket_items[bucket].append(
                self._data_provider.alphabet[letter_pos]
            )

    def get_runtime(self):
        """
        Store runtime of solving process
        """
        self.runtime = self._model.Runtime

    def write_dict_to_json(self, filepath, data_dict):
        with open(filepath, "w") as f:
            json.dump(data_dict, f, indent=4)

    def gurobi_status_code_mapping(self):
        """
        map numeric status code to status code string such as optimal
        return string
        """
        gurobi_status_code_map = {
            1: "LOADED",
            2: "OPTIMAL",
            3: "INFEASIBLE",
            4: "INF_OR_UNBD",
            5: "UNBOUNDED",
            6: "CUTOFF",
            7: "ITERATION_LIMIT",
            8: "NODE_LIMIT",
            9: "TIME_LIMIT",
            10: "SOLUTION_LIMIT",
            11: "INTERRUPTED",
            12: "NUMERIC",
            13: "SUBOPTIMAL",
            14: "INPROGRESS",
            15: "USER_OBJ_LIMIT",
            16: "WORK_LIMIT",
            17: "MEM_LIMIT",
        }
        return gurobi_status_code_map[self.status_code]

    def my_callback(self, model, where):
        if where == gp.GRB.Callback.MIP:
            try:
                best_bound = model.cbGet(gp.GRB.Callback.MIP_OBJBND)
                # only update if best bound is better than old one
                # maximization problem
                if self._best_current_bound > best_bound:
                    self._best_current_bound = best_bound
                    time_found = model.cbGet(gp.GRB.Callback.RUNTIME)
                    self.time_best_current_bound.append((time_found, best_bound))

            except gp.GurobiError as e:
                print(f"[MIP] Error getting bound: {e}")

        elif where == gp.GRB.Callback.MIPSOL:
            try:
                obj_val = model.cbGet(gp.GRB.Callback.MIPSOL_OBJ)
                time_found = model.cbGet(gp.GRB.Callback.RUNTIME)
                variables = model.getVars()
                var_values = model.cbGetSolution(variables)

                sparse_solution = {
                    var.varName: val
                    for var, val in zip(variables, var_values)
                    if abs(val) > 1e-6
                }

                self.intermediate_solutions.append(
                    (time_found, sparse_solution, obj_val)
                )

            except gp.GurobiError as e:
                print(f"[MIPSOL] Error: {e}")
