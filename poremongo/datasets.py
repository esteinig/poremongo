import os
import h5py
import json
import random
import logging
import numpy as np
import matplotlib

from tqdm import tqdm
from scipy.stats import sem
from sklearn.model_selection import train_test_split
from colorama import Fore, Style
from textwrap import dedent

from keras.utils import Sequence
from keras.utils.np_utils import to_categorical

from poremongo.models import Fast5

from pathlib import Path

matplotlib.use("agg")

import matplotlib.pyplot as plt
import matplotlib.style as style
import seaborn as sns

style.use("ggplot")


class AchillesDataset:
    def __init__(self, dataset=None, poremongo=None):

        self.dataset = dataset

        if dataset:
            self.dataset_handle = self.dataset.open()

        self.poremongo = poremongo


    def write_from_json(self, config: str or dict):

        """
        Sample multiple Datasets from a JSON file, where keys are
        the path of the output file,
        and values are dictionaries of parameters
         for the write function of Dataset.

        :param config:    JSON file with filename (key), write parameters dict with required parameter "tags" (values)

        :return:
        """

        if isinstance(config, str):
            with open(config, "r") as infile:
                config_dict = json.load(infile)
        else:
            config_dict = config

        if self.check_json_dict(config_dict):
            for file, params in config_dict.items():
                self.write(**params)

    @staticmethod
    def check_json_dict(config):

        allowed = (
            "data_file",
            "max_windows",
            "max_windows_per_read",
            "window_size",
            "window_step",
            "window_random",
            "window_recover",
            "sample_files_per_tag",
            "sample_proportions",
            "sample_unique",
            "scale",
            "validation",
            "chunk_size",
            "tags",
        )

        for fname, params in config.items():
            if "tags" not in params:
                raise ValueError(
                    f"Dataset entry must contains tags for sampling ({fname})."
                )
            for param, value in params.items():
                if param not in allowed:
                    raise ValueError(
                        f"Parameter '{param}' is not an allowed parameter ({fname})."
                    )

        return True

    @staticmethod
    def get_files_to_exclude(exclude_datasets):

        if exclude_datasets is not None:
            exclude = []
            for dataset in exclude_datasets:
                with h5py.File(dataset, "r") as infile:
                    try:
                        exclude += infile["data/files"]
                    except KeyError:
                        raise ValueError(
                            f"Could not detect path 'data/files' in file {dataset}"
                        )
            return exclude
        else:
            return list()

    @staticmethod
    def parse(data_file):

        with h5py.File(data_file, "r") as infile:
            return infile["data/files"], infile["data/labels"]

    def write(
        self,
        tags,
        data_file: Path or str ="data.h5",
        max_windows=20000,
        max_windows_per_read=50,
        window_size=400,
        window_step=0.1,
        window_random=True,
        window_recover=True,
        sample_files_per_tag=25000,
        sample_proportions=None,
        sample_unique=False,
        exclude_datasets=None,
        validation=0.3,
        scale=False,
        chunk_size=10000,
        max_reads=None,
        ssh=False,
        global_tags=None,
    ):

        """

         Re-work of the original function leaving core intact but
        building meta data and config into the HDF5 files.

        :param tags:
        :param data_file:
        :param sample_files_per_tag:
        :param sample_proportions:
        :param sample_unique:
        :param scale:
        :param max_windows:
        :param max_windows_per_read:
        :param window_size:
        :param window_step:
        :param window_random:
        :param window_recover:
        :param validation:
        :param chunk_size:
        :param ssh: explicit
        :param max_reads:
        :param global_tags:
        :param exclude_datasets
        :return:


        """

        if max_reads:
            max_windows = max_reads * max_windows_per_read

        if isinstance(window_step, float):
            window_step = int(window_size * window_step)

        if isinstance(exclude_datasets, str):
            exclude_datasets = [
                d for d in exclude_datasets.split(",")
            ]

        classes = len(tags)

        # Get list of Fast5 file names to exclude from sampling in PoreMongo
        exclude = self.get_uuids_to_exclude(exclude_datasets)

        with h5py.File(data_file, "w") as f:

            # Create data paths for storing all extracted data:
            data, labels, decoded, extracted = self.create_data_paths(
                file=f, window_size=window_size, classes=classes
            )

            self.print_write_summary(
                sample_files_per_tag=sample_files_per_tag,
                sample_proportions=sample_proportions,
                sample_unique=sample_unique,
                max_windows_per_read=max_windows_per_read,
                window_size=window_size,
                max_windows=max_windows,
                window_step=window_step,
                window_random=window_random,
            )

            # Each input directory corresponds to label (0, 1, 2, ...)
            for label, tag in enumerate(tags):

                files = self.poremongo.sample(
                    Fast5.objects,
                    tags=tag,
                    limit=sample_files_per_tag,
                    proportion=sample_proportions,
                    unique=sample_unique,
                    exclude_uuid=exclude,
                    include_tags=global_tags,
                    return_documents=True,
                )

                # Randomize, not necessary but precaution:
                random.shuffle(files)

                total = 0
                n_files = []

                with tqdm(total=max_windows) as pbar:
                    pbar.set_description(
                        f"Extracting tensors for label {label}"
                    )
                    for file in files:
                        # TODO: Multiple SSH connections
                        # in config; add tag for server to sampling?

                        if self.poremongo.scp is not None and ssh:
                            file.get(self.poremongo.scp)

                        # TODO: Template strand only, add second strand:
                        signal_windows = file.get_reads(
                            window_size=window_size,
                            window_step=window_step,
                            scale=scale,
                            template=True,
                            return_all=False,
                        )

                        # Remove the local copy of the file from SSH
                        if self.poremongo.scp is not None and ssh:
                            file.remove()

                        sampled_windows = self.sample_from_array(
                            signal_windows,
                            sample_size=max_windows_per_read,
                            random_sample=window_random,
                            recover=window_recover,
                        )

                        # Proceed if the maximum number of windows per class
                        # has not been reached,
                        # and if there are windows extracted from the Fast5:
                        if total < max_windows and sampled_windows.size > 0:
                            # If the number of extracted signal windows exceeds
                            # the difference between current total and
                            # max_windows is reached, cut off the signal window
                            # array and write it to file, to complete the loop
                            # for generating data for this label:
                            if sampled_windows.size > max_windows - total:
                                sampled_windows = sampled_windows[: max_windows - total]

                            # 4D input tensor (nb_samples, 1, signal_length, 1)
                            # for input to Residual Blocks
                            input_tensor = self.sample_to_input(sampled_windows)

                            # Write this tensor to file instead of storing in
                            # memory otherwise might raise OOM:
                            self.write_chunk(data, input_tensor)

                            # Operations for update to total number
                            # of windows processed for this label,
                            # tracking files from which signal
                            # is extracted, and updating progress bar:
                            nb_windows = input_tensor.shape[0]
                            total += nb_windows
                            pbar.update(nb_windows)
                            n_files.append(file.name)

                            # If the maximum number of signals
                            # for this class (label) has
                            # been reached, break the Fast5-file
                            # loop and proceed to writing
                            # label stored in memory (all at once)
                            if total >= max_windows - 1:
                                break

                print(f"Extracted {total} / {max_windows} windows for label {label}")

                # Writing all training labels to HDF5,
                # as categorical (one-hot) encoding:
                encoded_labels = to_categorical(
                    np.array([label for _ in range(total)]), classes
                )
                self.write_chunk(labels, encoded_labels)

                # Decoded (label-based) encoding for dataset summary:
                decoded_labels = np.array([label for _ in range(total)])
                self.write_chunk(decoded, decoded_labels)

                # Fast5 file paths from which signal arrays
                # were extracted for dataset summary:
                file_labels = np.array(
                    [fast5_file.encode("utf8") for fast5_file in n_files]
                )

                self.write_chunk(extracted, file_labels)

        self.print_data_summary(data_file=data_file)

        # TODO Randomize all again? for random vector in dataset file.
        # TODO while loop as memory guard in really large datasets

        if validation > 0:
            # Split dataset into training / validation data:
            self._training_validation_split(
                data_file=data_file,
                validation=validation,
                window_size=window_size,
                classes=classes,
                chunk_size=chunk_size,
            )

    def _training_validation_split(
        self,
        data_file,
        validation: float = 0.3,
        window_size: int = 400,
        classes: int = 2,
        chunk_size: int = 10000,
    ):

        """ This function takes a complete data set generated with write_data,
        randomizes the indices and splits it into training and validation
        under the paths training/data, training/label, validation/data,
         validation/label.

        :param validation
            proportion of data to be split into validation set
        :param window_size
            window (slice) size for writing data to training file in chunks
        :param classes
            number of classes (labels)
        :param chunk_size
            maximum number of windows for reading and writing in chunks
        """

        # Generate new file name for splitting data randomly into training and
        # validation data for input to AchillesModel (data_file + _training.h5)

        fname, fext = os.path.splitext(data_file)
        outfile = fname + ".training" + fext

        print("Splitting data into training and validation sets...\n")
        with h5py.File(data_file, "r") as data_file:

            # Get all indices for reading / writing in chunks:
            indices = np.arange(
                data_file["data/data"].shape[0]
            )

            # Randomize the indices from data/data and
            # split for training / validation:
            training_indices, validation_indices = train_test_split(
                indices, test_size=validation, random_state=None, shuffle=True
            )

            # Sanity checks for random and non-duplicated selection of indices:
            print(
                "Sample of randomized training indices:", training_indices[: 5]
            )
            print(
                "Sample of randomized validation indices:", validation_indices[: 5], "\n"
            )

            if set(training_indices).intersection(validation_indices):
                logging.debug(
                    "Training and validation data are "
                    "overlapping after splitting."
                )
                raise ValueError(
                    "Training and validation data are "
                    "overlapping after splitting."
                )

            with h5py.File(outfile, "w") as out:
                train_x, train_y, val_x, val_y = \
                    self.create_training_validation_paths(
                        file=out, window_size=window_size, classes=classes
                    )

                # Read and write the training / validation
                # data by chunks of indices that
                # correspond to the max_windows_per_read
                # parameter (minimum memory for processing)

                with tqdm(
                        total=len(training_indices)
                ) as pbar:

                    pbar.set_description("Writing training data")
                    for i_train_chunk in self.chunk(
                            training_indices, chunk_size
                    ):
                        self.write_chunk(
                            train_x, np.take(
                                data_file["data/data"], i_train_chunk, axis=0
                            ),
                        )
                        self.write_chunk(
                            train_y, np.take(
                                data_file["data/labels"], i_train_chunk, axis=0
                            ),
                        )
                        pbar.update(len(i_train_chunk))

                with tqdm(
                        total=len(validation_indices)
                ) as pbar:

                    pbar.set_description("Writing validation data")
                    for i_val_chunk in self.chunk(
                            validation_indices, chunk_size
                    ):
                        self.write_chunk(
                            val_x, np.take(
                                data_file["data/data"], i_val_chunk, axis=0
                            )
                        )
                        self.write_chunk(
                            val_y, np.take(
                                data_file["data/labels"], i_val_chunk, axis=0
                            ),
                        )
                        pbar.update(len(i_val_chunk))

                self.print_data_summary(data_file=outfile)

    @staticmethod
    def plot_signal_distribution(
        data_file,
        random_windows=True,
        nb_windows=10000,
        data_path="data",
        limit=(0, 300),
        histogram=False,
        bins=None,
        stats=True,
    ):

        """ Plotting function to generate signal value
         histograms for each category, sampled randomly
        this operates on the standard data path, but can
         be changed to training / validation data paths in HDF5
         """

        with h5py.File(data_file, "r") as data_file:
            # Get all indices from data path in HDF5
            indices = np.arange(data_file[data_path + "/data"].shape[0])
            # Randomize indices:
            if random_windows:
                np.random.shuffle(indices)

            # Select chunk size indices...
            indices = indices[:nb_windows]
            # ... and extract into memory:
            data_chunk = np.take(
                data_file[data_path + "/data"], indices, axis=0
            )
            label_chunk = np.take(
                data_file[data_path + "/labels"], indices, axis=0
            )
            # Transform one-hot encoded labels and get unique labels:
            all_labels = np.argmax(label_chunk, axis=1)
            # Labels should be integers starting at 0
            # , so sort them for plot legend:
            unique_labels = sorted(np.unique(all_labels))

            # For each label, extract corresponding
            # data chunk and flatten into simple array,
            # then plot as histogram or kernel density
            # estimate with Seaborn (easier to see):
            for label in unique_labels:
                i = np.where(all_labels == label)[0]
                # Extract data label-wise from chunk...
                data = np.take(data_chunk, i, axis=0)
                # ... then flatten into one-dimensional array:
                data = data.flatten()

                if limit:
                    # Print percentage of reads exceeding limits:
                    below_limit = round(len(data[data < limit[0]]), 6)
                    above_limit = round(len(data[data > limit[1]]), 6)
                    print(
                        "Limit warning: found {}% ({}) signal values < {} and "
                        "{}% ({}) signal values > {} for label {}".format(
                            round((below_limit / len(data)) * 100, 6),
                            below_limit,
                            limit[0],
                            round((above_limit / len(data)) * 100, 6),
                            above_limit,
                            limit[1],
                            label,
                        )
                    )
                    # Subset the data by limits:
                    data = data[(data > limit[0]) & (data < limit[1])]

                if stats:
                    mean = data.mean()
                    standard_error = sem(data)
                    print(
                        "Label {}: {} +- {}".format(
                            label, round(mean, 6), round(standard_error, 4)
                        )
                    )

                # Plot signal values:
                if histogram:
                    sns.distplot(data, kde=False, bins=bins)
                else:
                    sns.kdeplot(data, shade=True)

            plt.legend(unique_labels, title="Label")

    @staticmethod
    def chunk(seq, size):

        return (seq[pos: pos + size] for pos in range(0, len(seq), size))

    @staticmethod
    def create_data_paths(file, window_size=400, classes=2):

        # HDF5 file dataset creation
        # Window slices added to data path:
        data = file.create_dataset(
            "data/data",
            shape=(0, 1, window_size, 1),
            maxshape=(None, 1, window_size, 1),
        )
        # One-hot encoded labels
        labels = file.create_dataset(
            "data/labels", shape=(0, classes), maxshape=(None, classes)
        )

        # For data set summary only:
        dt = h5py.special_dtype(vlen=str)

        # Decoded labels (0, 1, 2, ...)
        decoded = file.create_dataset(
            "data/decoded", shape=(0,), maxshape=(None,)
        )
        # File paths sampled
        extracted = file.create_dataset(
            "data/uuids", shape=(0,), maxshape=(None,), dtype=dt
        )

        return data, labels, decoded, extracted

    @staticmethod
    def create_training_validation_paths(file, window_size=400, classes=2):

        data_paths = {"training": [], "validation": []}

        for data_type in data_paths.keys():
            # HDF5 file dataset creation:
            data = file.create_dataset(
                data_type + "/data",
                shape=(0, 1, window_size, 1),
                maxshape=(None, 1, window_size, 1),
            )

            labels = file.create_dataset(
                data_type + "/labels", shape=(0, classes), maxshape=(None, classes)
            )

            data_paths[data_type] += [data, labels]

        return (
            data_paths["training"][0],
            data_paths["training"][1],
            data_paths["validation"][0],
            data_paths["validation"][1],
        )

    @staticmethod
    def print_data_summary(data_file):

        print(
            dedent(
                f"""
        {Fore.YELLOW}Dataset (HD5)          {Fore.CYAN}{data_file}{Style.RESET_ALL}

        {Fore.YELLOW}Encoded label vector:  {Fore.MAGENTA}/data/labels{Style.RESET_ALL}
        {Fore.YELLOW}Decoded label vector:  {Fore.MAGENTA}/data/decoded{Style.RESET_ALL}
        {Fore.YELLOW}Extracted file names:  {Fore.MAGENTA}/data/files{Style.RESET_ALL}
        """
            )
        )

        with h5py.File(data_file, "r") as f:

            if "data/data" in f.keys():
                msg = dedent(
                    f"""
                    {Fore.YELLOW}Data file: {Fore.CYAN}{data_file}{Style.RESET_ALL}

                    {Fore.CYAN}Dimensions:{Style.RESET_ALL}

                    {Fore.GREEN}Data:       {Fore.YELLOW}{f["data/data"].shape}{Style.RESET_ALL}
                    {Fore.GREEN}Labels:     {Fore.YELLOW}{f["data/labels"].shape}{Style.RESET_ALL}
                    {Fore.GREEN}Fast5:      {Fore.YELLOW}{f["data/files"].shape}{Style.RESET_ALL}
                    """
                )

            elif "training/data" in f.keys() and "validation/data" in f.keys():
                msg = dedent(
                    f"""
                    {Fore.YELLOW}Data file: {Fore.CYAN}{data_file}{Style.RESET_ALL}

                    {Fore.CYAN}Training Dimensions:{Style.RESET_ALL}

                    {Fore.GREEN}Data:       {Fore.YELLOW}{f["training/data"].shape}{Style.RESET_ALL}
                    {Fore.GREEN}Labels:     {Fore.YELLOW}{f["training/labels"].shape}{Style.RESET_ALL}

                    {Fore.CYAN}Validation Dimensions:{Style.RESET_ALL}

                    {Fore.GREEN}Data:       {Fore.YELLOW}{f["validation/data"].shape}{Style.RESET_ALL}
                    {Fore.GREEN}Labels:     {Fore.YELLOW}{f["validation/labels"].shape}{Style.RESET_ALL}

                    """
                )
            else:
                logging.debug(
                    "Could not access either data/data or training/data + "
                    "validation/data in HDF5."
                )
                raise KeyError(
                    "Could not access either data/data or training/data + "
                    "validation/data in HDF5."
                )

            print(msg)

    @staticmethod
    def write_chunk(dataset, data):

        dataset.resize(dataset.shape[0] + data.shape[0], axis=0)
        dataset[-data.shape[0] :] = data

        return dataset

    @staticmethod
    def sample_from_array(
        array: np.array,
        sample_size: int,
        random_sample: bool = True,
        recover: bool = True,
    ) -> np.array:

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
            raise ValueError(
                f"Could not recover read array: number of read windows "
                f"({num_windows}) < sample size ({sample_size})"
            )

        if num_windows == sample_size:
            return array
        else:
            if random_sample:
                idx_max = num_windows - sample_size
                rand_idx = random.randint(0, idx_max)

                return array[rand_idx: rand_idx + sample_size]
            else:
                return array[:sample_size]

    @staticmethod
    def sample_to_input(array: np.array) -> np.array:

        """ Transform input array of (number_windows, window_size)
        to (number_windows, 1, window_size, 1)
        for input into convolutional layers:
        (samples, height, width, channels) in AchillesModel
        """

        if array.ndim != 2:
            raise ValueError(
                f"Array of shape {array.shape} "
                f"must conform to shape: (number_windows, window_size)"
            )

        return np.reshape(array, (array.shape[0], 1, array.shape[1], 1))

    def print_write_summary(self, **kwargs):

        print(
            dedent(
                f"""
        Generating data set for input to AchillesModel.
        =========================================================

        Sampling from PoreMongo:

            - {Fore.GREEN}sample_files_per_tag{Style.RESET_ALL}     {Fore.YELLOW}{kwargs["sample_files_per_tag"]}{Style.RESET_ALL} 
            - {Fore.GREEN}sample_proportions{Style.RESET_ALL}       {Fore.YELLOW}{kwargs["sample_proportions"]}{Style.RESET_ALL} 
            - {Fore.GREEN}sample_unique{Style.RESET_ALL}            {Fore.YELLOW}{kwargs["sample_unique"]}{Style.RESET_ALL} 

        Generating tensors of shape {Fore.YELLOW}({kwargs["max_windows_per_read"]}, 1, {kwargs["window_size"]}, 1){Style.RESET_ALL} per read.
        
        For each class, sample signal windows with the following parameters:

            - {Fore.GREEN}max_windows{Style.RESET_ALL}              {Fore.YELLOW}{kwargs["max_windows"]}{Style.RESET_ALL}
            - {Fore.GREEN}window_size{Style.RESET_ALL}              {Fore.YELLOW}{kwargs["window_size"]}{Style.RESET_ALL}
            - {Fore.GREEN}window_step{Style.RESET_ALL}              {Fore.YELLOW}{kwargs["window_step"]}{Style.RESET_ALL}
            - {Fore.GREEN}window_random{Style.RESET_ALL}            {Fore.YELLOW}{kwargs["window_random"]}{Style.RESET_ALL}             

        Fast5 models are shuffled by default after sampling.

        =========================================================
        """
            )
        )


class DataGenerator(Sequence):
    def __init__(
        self,
        data_file,
        data_type="training",
        batch_size=15,
        shuffle=True,
        no_labels=False,
    ):
        self.data_file = data_file
        self.data_type = data_type

        self.batch_size = batch_size
        self.shuffle = shuffle
        self.no_labels = no_labels

        self.indices = []

        self.data_shape, self.label_shape = self._get_data_shapes()

        self.on_epoch_end()

    def _get_data_shapes(self):
        with h5py.File(self.data_file, "r") as f:
            return (
                f[self.data_type + "/data"].shape,
                f[self.data_type + "/labels"].shape,
            )

    def __len__(self):
        """ Number of batches per epoch """

        return int(np.floor(self.data_shape[0]) / self.batch_size)

    def __getitem__(self, index):
        """ Generate one batch of data """

        # Generate indexes of the batch
        indices = self.indices[index * self.batch_size : (index + 1) * self.batch_size]

        # Generate data
        data, labels = self.__data_generation(indices)

        # Testing print statements:

        # print("Training data batch:", data.shape)
        # print("Training label batch:", labels.shape)
        # print("Generated data for indices:", indices)

        if self.no_labels:
            return data
        else:
            return data, labels

    def on_epoch_end(self):
        """ Updates indexes after each epoch """

        self.indices = np.arange(self.data_shape[0])

        if self.shuffle:
            np.random.shuffle(self.indices)

    def __data_generation(self, indices):
        """ Generates data containing batch_size samples """

        with h5py.File(self.data_file, "r") as f:
            file_data = f[self.data_type + "/data"]
            data = np.take(file_data, indices, axis=0)

            file_labels = f[self.data_type + "/labels"]
            labels = np.take(file_labels, indices, axis=0)

            return data, labels