"""
This class provides all data necessary to build the optimization problem
"""

import statistics
import math
import random


class OptimizationDataProvider:
    def __init__(self, config, alphabet):
        # text file containing line by line all words
        self._path_to_words = config["file_path_words"]
        # text file containing line by line all patterns
        self._path_to_patterns = config["file_path_patterns"]

        self.words = []  # list of words
        self.patterns = []  # list of patterns
        # dictionary: key pattern value tuple of two lists:
        # first list words containg pattern,
        # second list words not containing pattern
        self.dict_pattern_partition_words = {}
        # number of bins
        self.number_bins = config["number_of_bins"]
        # list of letters to partition to the bins
        self.alphabet = alphabet

        # for each pattern, translate string into position of alphabet (pattern_pos, pattern, positions)
        self.list_pattern_positions = []
        # key pattern, value positions
        self.dict_pattern_positions = {}
        # analogously for words
        self.list_word_positions = []
        self.dict_word_positions = {}

    def _read_lines_to_list(self, filepath):
        lines = []
        with open(filepath, "r", encoding="utf-8") as file:
            for line in file:
                lines.append(line.strip("\n"))
        return lines

    def parse_words_patterns(self):
        self.words = self._read_lines_to_list(self._path_to_words)
        self.patterns = self._read_lines_to_list(self._path_to_patterns)

    def determine_pattern_words_relation(self):
        """
        For each pattern and word, determine if pattern is part of the word.
        Store results in self._dict_pattern_partition_words[pattern][word] = True/False
        """
        self.dict_pattern_partition_words = {
            pattern: {word: pattern in word for word in self.words}
            for pattern in self.patterns
        }

    def index_set_pattern_letters(self):
        """
        For each pattern, translate the string into a list of
        positions in the alphabet.
        "ab" -> [0,1]
        """
        # Create a mapping: letter -> position (1-based)
        letter_to_pos = {letter: idx for idx, letter in enumerate(self.alphabet)}

        for pattern_pos, pattern in enumerate(self.patterns):
            positions = [letter_to_pos[char] for char in pattern]
            self.list_pattern_positions.append((pattern_pos, pattern, positions))
            self.dict_pattern_positions[pattern] = positions

    def index_set_word_letters(self):
        """
        For each word, translate the string into a list of
        positions in the alphabet.
        "ab" -> [0,1]
        """
        # Create a mapping: letter -> position (1-based)
        letter_to_pos = {letter: idx for idx, letter in enumerate(self.alphabet)}

        for word_pos, word in enumerate(self.words):
            positions = [letter_to_pos[char] for char in word]
            self.list_word_positions.append((word_pos, word, positions))
            self.dict_word_positions[word] = positions

    def subset_patterns_shuffled_block(self, seed, max_nr_pattern, nr_block):
        """
        Shuffle all patterns in self.patterns with a fixed seed.
        Return a subset block of patterns from the shuffled list.

        Parameters:
        - seed (int): Random seed for reproducibility.
        - max_nr_words (int): Number of patterns per block.
        - nr_block (int): Which block index to return (0-based).

        Returns:
        - List of patterns corresponding to the specified block.

        Raises:
        - IndexError: If the starting index is out of bounds.
        """
        # Copy and shuffle patterns
        patterns_copy = self.patterns[:]
        random.Random(seed).shuffle(patterns_copy)

        # If max_nr_words is too large, return all patterns
        if max_nr_pattern >= len(patterns_copy):
            return

        # Calculate block indices
        start_index = nr_block * max_nr_pattern
        end_index = (nr_block + 1) * max_nr_pattern

        # Raise an error if start index is out of bounds
        if start_index >= len(patterns_copy):
            raise IndexError(
                f"Block start index {start_index} is out of bounds for {len(patterns_copy)} patterns."
            )

        # Clamp end index to the length of the list
        end_index = min(end_index, len(patterns_copy))

        # overwrite patterns
        self.patterns = patterns_copy[start_index:end_index]

    def subset_words_shuffled_block(self, seed, max_nr_word, nr_block):
        """
        Shuffle all words in self.words with a fixed seed.
        Return a subset block of words from the shuffled list.

        Parameters:
        - seed (int): Random seed for reproducibility.
        - max_nr_words (int): Number of words per block.
        - nr_block (int): Which block index to return (0-based).

        Returns:
        - List of words corresponding to the specified block.

        Raises:
        - IndexError: If the starting index is out of bounds.
        """
        # Copy and shuffle words
        words_copy = self.words[:]
        random.Random(seed).shuffle(words_copy)

        # If max_nr_words is too large, return all words
        if max_nr_word >= len(words_copy):
            return

        # Calculate block indices
        start_index = nr_block * max_nr_word
        end_index = (nr_block + 1) * max_nr_word

        # Raise an error if start index is out of bounds
        if start_index >= len(words_copy):
            raise IndexError(
                f"Block start index {start_index} is out of bounds for {len(words_copy)} words."
            )

        # Clamp end index to the length of the list
        end_index = min(end_index, len(words_copy))

        # overwrite words
        self.words = words_copy[start_index:end_index]
