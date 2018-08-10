import numpy as np

from mongoengine import *

from ont_fast5_api.ont_fast5_api.fast5_info import Fast5Info
from ont_fast5_api.ont_fast5_api.fast5_file import Fast5File


class Read(EmbeddedDocument):

    id = StringField()
    number = IntField()

    channel = IntField()
    start_time = IntField()
    duration = IntField()

    digi = FloatField()
    range = FloatField()
    offset = FloatField()


class Fast5(Document):

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
    reads = EmbeddedDocumentListField(Read, required=False)

    # Classification
    tags = ListField(StringField(), required=False)
    comments = ListField(StringField(), required=False)

    # Meta
    meta = {"collection": "fast5"}

    # Single query API for Fast5

    def scan_file(self, update=True):

        """ Scans a Fast5 file and provides several pieces of data: info on reads (Read model) and
        tags on pass / fail if present in file path of Fast5. """

        info = Fast5Info(self.path)
        fast5 = Fast5File(self.path)

        reads = []
        for attr in info.read_info:
            channel_info = fast5.handle['UniqueGlobalKey/channel_id'].attrs

            read = Read(id=attr.read_id,
                        number=attr.read_number,
                        duration=attr.duration,
                        channel=info.channel,
                        start_time=attr.start_time,
                        digi=channel_info['digitisation'],
                        range=channel_info['range'],
                        offset=channel_info['offset'])

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
                        set__read_number=len(info.read_info))
        else:
            self.has_reads = True
            self.valid = info.valid
            self.version = info.version
            self.read_number = len(info.read_info)

    def get_signal(self, all_reads=False, scale=True):

        """ Scaled pA values (float32) or raw DAC values (int16), return first read (1D) or
        if all_reads = True return array of all signal arrays (e.g. if using 2D kits) """

        if all_reads:
            return np.array([Fast5File(self.path).get_raw_data(attr.read_number, scale=scale)
                             for attr in Fast5Info(self.path).read_info])
        else:
            return Fast5File(self.path).get_raw_data(scale=scale)

    def add_tag(self, tag):

        return self.update(add_to_set__tags=tag)

    def add_comment(self, comment):

        return self.update(add_to_set__comments=comment)

    def remove_tag(self, tag):

        return self.update(pull__tags=tag)

    def remove_comment(self, comment):

        return self.update(pull__comments=comment)





