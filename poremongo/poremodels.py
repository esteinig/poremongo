import numpy as np

from datetime import datetime
from pathlib import Path

import time

from mongoengine import *
from skimage.util import view_as_windows

from ont_fast5_api.multi_fast5 import MultiFast5File
from ont_fast5_api.fast5_read import Fast5Read

from colorama import Fore

Y = Fore.YELLOW
G = Fore.GREEN
B = Fore.BLUE
RE = Fore.RESET


def timestamp_to_epoch(timestamp: float) -> float:
    """Auxiliary function to parse timestamp into epoch time."""

    epoch = datetime(1970, 1, 1)
    time_as_date = datetime.fromtimestamp(timestamp)
    return (time_as_date - epoch).total_seconds()


def epoch_to_timestamp(epoch_seconds: float) -> str:
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(epoch_seconds))


class Read(Document):

    _id = ObjectIdField()  # state explicit for construction from dicts
    uuid = StringField(required=True, unique=True)  # public identifier assigned by poremongo

    fast5 = StringField(required=True, unique=False)  # path to Fast5 file read located in
    tags = ListField(StringField())  # list of tags for this read

    read_id = StringField()  # internal id of read
    signal_data = ListField(IntField())  # signal data array
    signal_data_length = IntField  # numbero f signals in array

    meta = {"collection": "fast5"}

    is_copy: bool = False
    pretty_print: bool = False

    def __str__(self):

        if self.pretty_print:
            return f"{Y}@{RE}{self.read_id}\t{f'{G}::{RE}'.join(self.tags)}"
        else:
            return f"@{self.read_id}\t{self.uuid}\t{'; '.join(self.tags)}\t{self.fast5}"

    def get_fast5(self, scp_client, out_dir=".", tmp_dir=None, prefix=None):

        """ Get the Fast5 file that hosts the read into local storage via SCP"""

        if out_dir:
            tmp_dir = Path(out_dir)
        else:
            tmp_dir = Path(tmp_dir)

        fast5_path = Path(self.fast5)

        tmp_fast5 = tmp_dir / fast5_path.name

        if not tmp_fast5.exists():
            scp_client.get(self.fast5, local_path=tmp_dir)
        else:
            pass

        if prefix:
            tmp_fast5.rename(tmp_fast5.parent / f"{prefix}_{tmp_fast5.stem}_{tmp_fast5.suffix}")

        # This will set the document path of the locally
        # used object to the local file transferred via SCP
        # This will not update the database!
        self.fast5 = str(tmp_fast5)
        self.is_copy = True

    def remove_fast5(self):

        """ Remove a temporary local copy of the Fast5 file """

        # Only remove the file if it is a local copy!
        if self.is_copy:
            local_path = Path(self.fast5)
            if local_path.exists():
                local_path.unlink()
            else:
                pass
        else:
            raise ValueError(
                "Model path must have been modified by self.get - "
                "otherwise this risks deleting the master copy "
                "of this Fast5 in local storage."
            )

    def get_signal(
        self,
        start: int = None,
        end: int = None,
        scale: bool = False,
        window_size: int = None,
        window_step: int = None
    ) -> np.array:

        """ Scaled pA values (float32) or raw data acquisition values (int16) """

        fast5: MultiFast5File = MultiFast5File(self.fast5)
        signal_read: Fast5Read = fast5.get_read(read_id=self.read_id)
        raw_signal: np.array = signal_read.get_raw_data(start=start, end=end, scale=scale)

        # Windows will only return full-sized windows,
        # incomplete windows at end of read are not included -
        # this is necessary for complete tensors in training and prediction:

        if window_size and window_step:
            return np.array(
                view_as_windows(raw_signal, window_shape=window_size, step=window_step)
            )
        else:
            return raw_signal

