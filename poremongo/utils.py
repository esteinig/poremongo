from random import randint
import numpy as np


def sample_to_input(array: np.array) -> np.array:

    """Transform input array of (number_windows, window_size) to
    (number_windows, 1, window_size, 1) for input into convolutional
    layer in resblock: (samples, height, width, channels) in Achilles
    """

    if array.ndim != 2:
        raise ValueError(f"Array of shape {array.shape} must conform to shape: (number_windows, window_size)")

    return np.reshape(array, (array.shape[0], 1, array.shape[1], 1))


def sample_from_array(array: np.array, sample_size: int, random_sample: bool=True, recover: bool=True) -> np.array:

    """Return a contiguous sample from an array of signal windows
    :param array
    :param sample_size
    :param random_sample
    :param recover
    """

    num_windows = array.shape[0]

    if num_windows < sample_size and recover:
        return array

    if num_windows < sample_size and not recover:
        raise ValueError(f"Could not recover read array: number of read windows "
                         f"({num_windows}) < sample size ({sample_size})")

    if num_windows == sample_size:
        return array
    else:
        if random_sample:
            idx_max = num_windows - sample_size
            rand_idx = randint(0, idx_max)

            return array[rand_idx:rand_idx+sample_size]
        else:
            return array[:sample_size]


