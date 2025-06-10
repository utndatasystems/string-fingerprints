"""
Building the Gurobi Model
"""

import gurobipy as gp


class GurobiModelBuilder:
    def __init__(self, data_provider, config):
        # class contain all data for the optimization model
        self._data_provider = data_provider
        # gurobi model
        env = gp.Env(empty=True)
        env.start()
        self._model = gp.Model(env=env)
        # bool_linearize: True -> we linearize binary products on our own else not
        self._bool_linearize = config["bool_linearize"]
        # used variables
        self.x_vars = self._id_p = self._eta_vars = None

        self._nr_patterns = len(self._data_provider.patterns)
        self._nr_words = len(self._data_provider.words)
        self._number_bins = self._data_provider.number_bins
        # index sets

        # (word_pos, pattern_pos) for pattern word combinations for which pattern is not in word
        # word_pos means positition in self.words, analogously pattern_pos
        self._word_pos_pattern_pos_not_included = list(
            set(
                (word_pos, pattern_pos)
                for pattern_pos, pattern in enumerate(self._data_provider.patterns)
                for word_pos, word in enumerate(self._data_provider.words)
                if not self._data_provider.dict_pattern_partition_words[pattern][word]
            )
        )
        # add to above also corresponding bins (word_pos, pattern_pos, bin_pos)
        self._word_pos_pattern_pos_bin_pos_not_included = [
            (word, pos, bin_idx)
            for word, pos in self._word_pos_pattern_pos_not_included
            for bin_idx in range(self._number_bins)
        ]
        # triples (pattern_pos, letter_pos, bin_pos) with
        # pattern_pos position in self._data_provider.patterns
        # letter_pos is of pattern(pattern_pos) on letter in its numeric value, e.g.,
        # pattern = ac und pattern is first pattern and one bin
        # (0,0,0), (0,2,0)
        self._pattern_pos_letter_pos_bin_pos = list(
            set(
                (pattern_pos, letter_pos, bin_pos)
                for pattern_pos, pattern, letter_positions in self._data_provider.list_pattern_positions
                for letter_pos in letter_positions
                for bin_pos in range(self._number_bins)
            )
        )

        # analogously we do the same for the words
        self._word_pos_letter_pos_bin_pos = list(
            set(
                (word_pos, letter_pos, bin_pos)
                for word_pos, word, letter_positions in self._data_provider.list_word_positions
                for letter_pos in letter_positions
                for bin_pos in range(self._number_bins)
            )
        )

    def _add_variables(self):
        """
        Add all optimization variables
        """
        # add binary variables x for partitioning letters to bins
        # first index: position of letter in alphabet -> avoid special characters in string
        # second index: possible bin
        self.x_vars = self._model.addVars(
            range(0, len(self._data_provider.alphabet)),
            range(0, self._data_provider.number_bins),
            vtype=gp.GRB.BINARY,
            name="x",
        )

        # for each pattern add binary variables id_p that encode
        # the string according to bins/buckets
        # Add all binary variables for the patterns
        self._id_p = self._model.addVars(
            self._nr_patterns,
            range(0, self._number_bins),
            vtype=gp.GRB.BINARY,
            name="id_p",
        )

        # analogously do this for each word: with a variable id_w
        self._id_w = self._model.addVars(
            self._nr_words,
            range(0, self._number_bins),
            vtype=gp.GRB.BINARY,
            name="id_w",
        )

        # binary variables identifying if we correctly classified a
        # (pattern, word) combination, i.e., binary 0 if model correctly
        # determines if pattern is in word, else false positive
        self._eta_vars = self._model.addVars(
            self._word_pos_pattern_pos_not_included,
            vtype=gp.GRB.BINARY,
            name="eta",
        )

        # add helper variables for linearizing
        if self._bool_linearize:
            self._helper_lin_vars = self._model.addVars(
                self._word_pos_pattern_pos_bin_pos_not_included,
                vtype=gp.GRB.CONTINUOUS,
                lb=0.0,
                ub=1.0,
                name="helper_lin",
            )

    def _add_constraints(self):

        # each letter only in one bucket
        self._model.addConstrs(
            (
                gp.quicksum(self.x_vars[i, j] for j in range(self._number_bins)) == 1
                for i in range(len(self._data_provider.alphabet))
            ),
            name="letter_to_bin",
        )

        # constraints encoding id for patterns, i.e.,
        # which buckets are set to one for a given pattern
        # constraints indices pattern_pos, letter_pos, bin_pos and exclude duplicates

        self._model.addConstrs(
            (
                self.x_vars[letter_pos, bin_pos] <= self._id_p[pattern_pos, bin_pos]
                for pattern_pos, letter_pos, bin_pos in self._pattern_pos_letter_pos_bin_pos
            ),
            name="id_pattern_PartOne",
        )

        # pattern id and bin number
        self._model.addConstrs(
            (
                self._id_p[pattern_pos, bin_pos]
                <= gp.quicksum(
                    self.x_vars[letter_pos, bin_pos]
                    for letter_pos in self._data_provider.dict_pattern_positions[
                        pattern
                    ]
                )
                for pattern_pos, pattern in enumerate(self._data_provider.patterns)
                for bin_pos in range(0, self._number_bins)
            ),
            name="id_pattern_PartTwo",
        )

        # analogously constraints encoding id for words, i.e.,
        # which buckets are set to one for a given word
        # constraints indices word_pos, letter_pos, bin_pos and exclude duplicates
        self._model.addConstrs(
            (
                self.x_vars[letter_pos, bin_pos] <= self._id_w[word_pos, bin_pos]
                for word_pos, letter_pos, bin_pos in self._word_pos_letter_pos_bin_pos
            ),
            name="id_word_PartOne",
        )

        self._model.addConstrs(
            (
                self._id_w[word_pos, bin_pos]
                <= gp.quicksum(
                    self.x_vars[letter_pos, bin_pos]
                    for letter_pos in self._data_provider.dict_word_positions[word]
                )
                for word_pos, word in enumerate(self._data_provider.words)
                for bin_pos in range(0, self._number_bins)
            ),
            name="id_word_PartTwo",
        )

        print("Redundant constraints are not implemented")

        # implement false positive constraints
        if self._bool_linearize:

            self._model.addConstrs(
                (
                    self._helper_lin_vars[word_pos, pattern_pos, bin_pos]
                    <= (1 - self._id_w[word_pos, bin_pos])
                    for bin_pos in range(0, self._number_bins)
                    for word_pos, pattern_pos in self._word_pos_pattern_pos_not_included
                ),
                name="helper_lin_cons_One",
            )
            self._model.addConstrs(
                (
                    self._helper_lin_vars[word_pos, pattern_pos, bin_pos]
                    <= self._id_p[pattern_pos, bin_pos]
                    for bin_pos in range(0, self._number_bins)
                    for word_pos, pattern_pos in self._word_pos_pattern_pos_not_included
                ),
                name="helper_lin_cons_Two",
            )

            self._model.addConstrs(
                (
                    self._eta_vars[word_pos, pattern_pos]
                    <= gp.quicksum(
                        self._helper_lin_vars[word_pos, pattern_pos, bin_pos]
                        for bin_pos in range(0, self._number_bins)
                    )
                    for word_pos, pattern_pos in self._word_pos_pattern_pos_not_included
                ),
                name="helper_lin_cons_Three",
            )

        else:
            self._model.addConstrs(
                (
                    self._eta_vars[word_pos, pattern_pos]
                    <= gp.quicksum(
                        (1 - self._id_p[pattern_pos, bin_pos])
                        * self._id_w[word_pos, bin_pos]
                        for bin_pos in range(0, self._number_bins)
                    )
                    for word_pos, pattern_pos in self._word_pos_pattern_pos_not_included
                ),
                name="false_positive",
            )

    def _add_objective(self):
        # add vars in obj for pattern word combinations with pattern is not in word
        # max sum of eta variables
        self._model.setObjective(
            gp.quicksum(
                self._eta_vars[w, p] for w, p in self._word_pos_pattern_pos_not_included
            ),
            sense=gp.GRB.MAXIMIZE,
        )

    def build_model(self):
        self._add_variables()
        self._add_constraints()
        self._add_objective()
