import os
import numpy as np

from mongoengine import *
from skimage.util import view_as_windows

from ont_fast5_api.fast5_info import Fast5Info
from ont_fast5_api.fast5_file import Fast5File

from datetime import datetime


class Read(EmbeddedDocument):

    id = StringField()
    number = IntField()

    channel = IntField()
    start_time = FloatField()
    duration = IntField()

    digi = FloatField()
    range = FloatField()
    offset = FloatField()

    end_time = FloatField()

    def statistics(self):

        pass


class Fast5(Document):

    _id = ObjectIdField()

    # File Info
    path = StringField(unique=True, required=True)
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

    reads = EmbeddedDocumentListField(Read, required=False)

    # Classification
    tags = ListField(StringField(), required=False)
    comments = ListField(StringField(), required=False)

    # Meta
    meta = {"collection": "fast5"}

    copy = False

    # Single query API for Fast5

    def get(self, scp_client, out_dir=".", temp_dir=None, prefix=None):

        """ Change the path attribute of the instance to copy the file as
        temporary file from remote host via SSH.

        """

        if out_dir:
            tmp_dir = os.path.abspath(out_dir)
        else:
            tmp_dir = os.path.abspath(temp_dir)

        tmp_fast5 = os.path.join(tmp_dir, self.name)

        scp_client.get(self.path, local_path=tmp_dir)

        if prefix:
            os.rename(tmp_fast5, os.path.join(tmp_dir, prefix+"_"+self.name))

        self.path = tmp_fast5
        self.copy = True

    def remove(self):

        """ Remove a temporary local copy of the Fast5 file """

        try:
            if self.copy:
                os.remove(self.path)
            else:
                raise ValueError("Model path must have been modified by self.get - otherwise this risks"
                                 "deleting the master copy of this Fast5 in local storage.")
        except OSError:
            pass

    def scan_file(self, update=True):

        """ Scans a Fast5 file and provides several pieces of data: info on reads (Read model) and
        tags on pass / fail if present in file path of Fast5. """

        info = Fast5Info(self.path)
        fast5 = Fast5File(self.path)

        exp_start_time, sampling_rate = self._get_time_and_sampling_rate(fast5)

        reads = []
        for attr in info.read_info:
            channel_info = fast5.handle['UniqueGlobalKey/channel_id'].attrs

            read = Read(id=attr.read_id,
                        number=attr.read_number,
                        duration=int(attr.duration),
                        channel=int(info.channel),
                        start_time=int(attr.start_time),
                        digi=channel_info['digitisation'],
                        range=channel_info['range'],
                        offset=channel_info['offset'])

            # Add compute of timestamp when read finishes compared t
            read.end_time = self.calculate_timestamp(read, exp_start_time, sampling_rate)

            if update:
                self.update(add_to_set__reads=read)
            else:
                reads.append(read)

        if not update:
            self.reads = reads

        if update:
            self.update(set__has_reads=True,
                        set__valid=info.valid,
                        set__version=info.version,
                        set__read_number=len(info.read_info),
                        set__sampling_rate=sampling_rate,
                        set__exp_start_time=exp_start_time)
        else:
            self.has_reads = True
            self.valid = info.valid
            self.version = info.version
            self.read_number = len(info.read_info)
            self.sampling_rate = sampling_rate
            self.exp_start_time = exp_start_time

    def _get_time_and_sampling_rate(self, fast5):

        exp_start_time = fast5.get_tracking_id().get('exp_start_time')

        try:
            exp_start_time = float(exp_start_time)
        except ValueError:
            pass
        try:
            exp_start_time = float(datetime.strptime(exp_start_time, "%Y-%m-%dT%H:%M:%SZ").timestamp())
        except TypeError:
            pass

        exp_start_time = self._timestamp_to_epoch_time(exp_start_time)
        sampling_rate = float(fast5.get_channel_info().get('sampling_rate'))

        return exp_start_time, sampling_rate

    def get_reads(self, window_size: int=None, window_step: int=None, scale: bool=False, template: bool=True,
                  return_all=True):

        """ Scaled pA values (float32) or raw DAC values (int16), return first read (1D) or
        if all_reads = True return array of all signal arrays (e.g. if using 2D kits) """

        if template:
            reads = np.array([Fast5File(self.path).get_raw_data(scale=scale)])
        else:
            reads = np.array([Fast5File(self.path).get_raw_data(attr.read_number, scale=scale)
                              for attr in Fast5Info(self.path).read_info])

        # Windows will only return full-sized windows, incomplete windows at end of read are not included -
        # this is necessary for complete tensors in training and prediction:

        if window_size and window_step:
            reads = np.array([view_as_windows(read, window_shape=window_size, step=window_step) for read in reads])

        if return_all:
            return reads
        else:
            if len(reads) > 0:
                return reads[0]
            else:
                raise ValueError("No reads in array.")

    @staticmethod
    def _timestamp_to_epoch_time(timestamp: float) -> float:
        """Auxiliary function to parse timestamp into epoch time."""
        epoch = datetime(1970, 1, 1)
        time_as_date = datetime.fromtimestamp(timestamp)
        return (time_as_date - epoch).total_seconds()

    @staticmethod
    def calculate_timestamp(read, exp_start_time, sampling_rate) -> float:
        """Calculates the epoch time when the read finished sequencing.
        :param read: full path to fast5 file
        :returns Epoch time that the read finished sequencing
        """
        if exp_start_time is None:  # missing field(s) in fast5 file
            return 0.0
        experiment_start = exp_start_time
        # adjust for the sampling rate of the channel
        sample_length = read.start_time + read.duration
        finish = sample_length / sampling_rate

        return experiment_start + finish


