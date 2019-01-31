from itertools import product


ALLOWABLE_CHARS = set(range(10))


def check_sudoku_board(board):
    """
    Checks whether a given sudoku board is valid. A valid board must be 9x9 and can only contain 0-9

    1 0 0 0 0 7 0 9 0
    0 3 0 0 2 0 0 0 8
    0 0 9 6 0 0 5 0 0
    0 0 5 3 0 0 9 0 0
    0 1 0 0 8 0 0 0 2
    6 0 0 0 0 4 0 0 0
    3 0 0 0 0 0 0 1 0
    0 4 0 0 0 0 0 0 7
    0 0 7 0 0 0 3 0 0

    There are three groups to check: row, column, and 3x3 subgroup.
    Each group must have distinct values of 1-9, however there can be multiple 0's since that will represent
    and unfinished entry.


    :param board: int[][] (a list of lists)
    :return: boolean
    """

    if not is_board_valid(board):
        return None

    return all([
        has_valid_rows(board),
        has_valid_columns(board),
        has_valid_subgroups(board),
    ])


def has_valid_subgroups(board):
    """
    Checks whether the subgroups in a board are correct.
    """
    for row, column in product([0, 3, 6], [0, 3, 6]):
        subgroup = []
        for x in range(row, row + 3):
            for y in range(column, column + 3):
                subgroup.append(board[x][y])
        if not is_valid_group(subgroup):
            return False
    return True


def has_valid_rows(board):
    """
    Checks whether the rows in the board are correct.
    """
    for row in board:
        if not is_valid_group(row):
            return False
    return True


def has_valid_columns(board):
    """
    Checks whether the columns in the board are correct.
    """
    columns = zip(*board)
    for column in columns:
        if not is_valid_group(column):
            return False
    return True


def is_valid_group(group):
    """
    Given a group as a list, return if there are distinct integers (1-9).
    There can be any number of 0s.

    Example: [1, 2, 3, 4, 4, 5, 6, 7, 9] is invalid.
    Example: [0, 0, 1, 0, 0, 3, 0, 2, 0] is valid.

    :param group: int[]
    :return: boolean
    """
    unique_values = set()

    for item in group:
        if item in unique_values and item != 0:
            return False
        unique_values.add(item)
    return True


def is_board_valid(board):
    """
    Given a board, check whether the board is 9x9 and contains only 0-9 as entries.
    """
    if len(board) != 9:
        return False

    for row in board:
        if len(row) != 9:
            return False
        for char in row:
            if char not in ALLOWABLE_CHARS:
                return False
    return True
