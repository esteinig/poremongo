import os
import time
import numpy as np

from mongoengine import *
from skimage.util import view_as_windows

from ont_fast5_api.fast5_info import Fast5Info
from ont_fast5_api.fast5_file import Fast5File

from datetime import datetime

# TODO: Paramiko Timeout


def timestamp_to_epoch(timestamp: float) -> float:
    """Auxiliary function to parse timestamp into epoch time."""

    epoch = datetime(1970, 1, 1)
    time_as_date = datetime.fromtimestamp(timestamp)
    return (time_as_date - epoch).total_seconds()


def epoch_to_timestamp(epoch_seconds: float) -> str:
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(epoch_seconds))


class Sequence(EmbeddedDocument):

    id = StringField(required=True)

    workflow = StringField(required=True)

    basecaller = StringField(required=True)
    version = StringField(required=True)

    taxdb = StringField()
    taxid = IntField()

    path = StringField()
    name = StringField()
    dir = StringField()

    quality = FloatField()
    length = IntField()


class Read(EmbeddedDocument):

    id = StringField()
    number = IntField()

    channel = IntField()
    start_time = FloatField()
    duration = IntField()

    digitisation = FloatField()
    range = FloatField()
    offset = FloatField()

    end_time = FloatField()

    sequences = EmbeddedDocumentListField(Sequence)

    def statistics(self):

        pass


class Fast5(Document):

    _id = ObjectIdField()
    uuid = StringField(required=False, unique=True)

    # File Info - Change unique here to induce break
    # in insertion of the same files
    path = StringField(required=True, unique=False)
    name = StringField(required=True)
    dir = StringField(required=False)

    # Fast5 Info
    valid = BooleanField(required=False)
    version = FloatField(required=False)

    # Signal Read Info
    has_reads = BooleanField(required=False)
    read_number = IntField(required=False)
    sampling_rate = FloatField(required=False)
    exp_start_time = FloatField(required=False)

    # TODO: run_id (to separate runs)

    reads = EmbeddedDocumentListField(Read, required=False)

    # Classification
    tags = ListField(StringField(), required=False)

    # Meta
    meta = {"collection": "fast5"}

    is_copy: bool = False

    # Single query API for Fast5

    def get(self, scp_client, out_dir=".", temp_dir=None, prefix=None):

        """ Change the path attribute of the instance
        to copy the file as temporary file from remote
        host via SSH.
        """

        if out_dir:
            tmp_dir = os.path.abspath(out_dir)
        else:
            tmp_dir = os.path.abspath(temp_dir)

        tmp_fast5 = os.path.join(tmp_dir, str(self.name))

        scp_client.get(self.path, local_path=tmp_dir)

        if prefix:
            os.rename(tmp_fast5, os.path.join(tmp_dir, prefix+"_"+self.name))

        self.path = tmp_fast5
        self.is_copy = True

    def remove(self):

        """ Remove a temporary local copy of the Fast5 file """

        try:
            if self.is_copy:
                os.remove(self.path)
            else:
                raise ValueError(
                    "Model path must have been modified by self.get - "
                    "otherwise this risks deleting the master copy "
                    "of this Fast5 in local storage."
                )
        except OSError:
            pass

    def scan_file(self, update=True):

        """ Scans Fast5 files for data to exract into model"""

        info = Fast5Info(self.path)
        fast5 = Fast5File(self.path)

        exp_start_time, sampling_rate = self._get_time_and_sampling_rate(fast5)
        reads = self._create_reads(
            info=info,
            fast5=fast5,
            sampling_rate=sampling_rate,
            exp_start_time=exp_start_time,
            update=update
        )

        if not update:
            self.reads = reads

        if update:
            self.update(
                set__has_reads=True,
                set__valid=info.valid,
                set__version=info.version,
                set__read_number=len(info.read_info),
                set__sampling_rate=sampling_rate,
                set__exp_start_time=exp_start_time
            )
        else:
            self.has_reads = True
            self.valid = info.valid
            self.version = info.version
            self.read_number = len(info.read_info)
            self.sampling_rate = sampling_rate
            self.exp_start_time = exp_start_time

    def _create_reads(
        self,
        info: Fast5Info,
        fast5: Fast5File,
        sampling_rate: float,
        exp_start_time: float,
        update: bool = False
    ) -> list:

        reads = []
        for attr in info.read_info:
            channel_info = fast5.handle['UniqueGlobalKey/channel_id'].attrs

            read = Read(
                id=attr.read_id,
                number=attr.read_number,
                duration=int(attr.duration),
                channel=int(info.channel),
                start_time=int(attr.start_time),
                end_time=self.calculate_timestamp(
                    int(attr.start_time),
                    int(attr.duration),
                    exp_start_time,
                    sampling_rate
                ),
                digitisation=channel_info['digitisation'],
                range=channel_info['range'],
                offset=channel_info['offset']
            )

            if update:
                self.update(add_to_set__reads=read)
            else:
                reads.append(read)

        return reads

    def _get_time_and_sampling_rate(self, fast5) -> (float, float):

        exp_start_time = fast5.get_tracking_id().get('exp_start_time')

        try:
            exp_start_time = float(exp_start_time)
        except ValueError:
            pass
        try:
            exp_start_time = float(
                datetime.strptime(
                    exp_start_time, "%Y-%m-%dT%H:%M:%SZ"
                ).timestamp()
            )
        except TypeError:
            pass

        exp_start_time = timestamp_to_epoch(exp_start_time)

        sampling_rate = float(
            fast5.get_channel_info().get('sampling_rate')
        )

        return exp_start_time, sampling_rate

    def get_reads(
        self,
        window_size: int = None,
        window_step: int = None,
        scale: bool = False,
        template: bool = True
    ) -> np.array:

        """ Scaled pA values (float32) or raw DAQ values (int16),
        return array of length 1 (1D) or array of length 2 (2D) """

        if template:
            reads = np.array(
                [Fast5File(self.path).get_raw_data(scale=scale)]
            )
        else:
            reads = np.array(
                [Fast5File(self.path).get_raw_data(
                    attr.read_number, scale=scale
                ) for attr in Fast5Info(self.path).read_info]
            )

        # Windows will only return full-sized windows,
        # incomplete windows at end of read are not included -
        # this is necessary for complete tensors in training and prediction:

        if window_size and window_step:
            reads = np.array(
                [view_as_windows(
                    read, window_shape=window_size, step=window_step
                ) for read in reads]
            )

        if not reads:
            raise ValueError('No reads in array.')

        return reads

    @staticmethod
    def calculate_timestamp(
            read_start_time, read_duration, exp_start_time, sampling_rate
    ) -> float:

        """Calculates the epoch time when the read finished sequencing.
        :param read_start_time:
        :param read_duration:
        :param exp_start_time:
        :param sampling_rate:
        :returns Epoch time that the read finished sequencing
        """
        if exp_start_time is None:  # missing field(s) in fast5 file
            return 0.0
        experiment_start = exp_start_time
        # adjust for the sampling rate of the channel
        sample_length = read_start_time + read_duration
        finish = sample_length / sampling_rate

        return experiment_start + finish

    def to_row(self, sep: str = '\t'):

        print(
            self.uuid, sep,
            str(self.read_number)+'D', sep,
            ' '.join(self.tags), sep,
            f' '.join([str(r.number) for r in self.reads]), sep,
            epoch_to_timestamp(self.exp_start_time)
        )
        #
        # return f"{str(self.uuid)}{sep}{str(self.read_number)}D{sep}" \
        #     f"{sep}{':'.join(self.tags)}{sep}" \
        #     f"{f'{sep}'.join([r.number for r in self.reads])}{sep}"\
        #     f"{epoch_to_timestamp(self.exp_start_time)}{sep}" \



