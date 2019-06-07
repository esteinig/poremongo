""" Basically PoreDB for Fast5 management in MongoDB """

import os
import time
import math
import tqdm
import json
import random
import shutil
import pymongo
import pandas
import logging
import paramiko

import multiprocessing as mp

from scp import SCPClient
from pathlib import Path
from functools import reduce
from operator import or_, and_
from datetime import datetime, timedelta

from mongoengine import connect
from mongoengine.queryset.visitor import Q
from pymongo.errors import ServerSelectionTimeoutError

from apscheduler.schedulers.background import BackgroundScheduler

from poremongo.watchdog import FileWatcher
from poremongo.models import Fast5, Read, Sequence
from poremongo.utils import _get_doc, _insert_doc

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(name)-12s] \t %(message)s",
    datefmt='%H:%M:%S'
)


class PoreMongo:

    """ API for PoreMongo """

    def __init__(
        self,
        uri: str = None,
        config: Path or dict = None,
        connect: bool = False,
        ssh: bool = False,
        mock: bool = False
    ):
        disallowed = ['local']
        if Path(uri).stem in disallowed:
            raise ValueError(f"Database can not be named: "
                             f"{', '.join(disallowed)}")

        self.uri = uri
        self.verbose = True

        self.logger = logging.getLogger(__name__)

        if config:
            self._parse_config(config)

        self.mock = mock

        self.ssh = None
        self.scp = None

        self.client: pymongo.MongoClient = pymongo.MongoClient(None)
        self.db = None  # Client DB
        self.fast5 = None  # Fast5 collection

        if connect:
            self.connect(ssh=ssh, is_mock=mock)

    def _parse_config(self, config: Path or dict):

        if isinstance(config, Path):
            with config.open('r') as cfg:
                config_dict = json.load(cfg)
                self.config = config_dict
                try:
                    self.uri = config_dict["uri"]
                except KeyError:
                    raise KeyError(
                        "Configuration dictionary must contain key 'uri' "
                        "to make the connection to MongoDB."
                    )
        elif isinstance(config, dict):
            try:
                self.uri = config["uri"]
                self.config = config
            except KeyError:
                raise KeyError(
                    "Configuration dictionary must contain key 'uri' "
                    "to make the connection to MongoDB."
                )
        else:
            raise ValueError(
                "Config must be string path to JSON file or dictionary."
            )

    def is_connected(self):

        return True if self.client else False

    def connect(self, ssh: bool = False, is_mock: bool = False, **kwargs):

        self.logger.debug(
            f'Attempting to connect to: {self.decompose_uri()}'
        )

        try:
            self.client = connect(
                host=self.uri,
                serverSelectionTimeoutMS=10000,
                is_mock=is_mock,
                **kwargs
            )
        except ServerSelectionTimeoutError:
            raise

        self.logger.info(
            f'Success! Connected to: {self.decompose_uri()}'
        )

        self.db = self.client.db    # Database connected
        self.fast5 = self.db.fast5  # Fast5 collection

        self.logger.info(
            'Default collection for PoreMongo is >> fast5 <<'
        )

        if ssh:
            self.logger.debug(
                'Attempting to open SSH and SCP'
            )
            self.open_ssh()
            self.open_scp()
            self.logger.info(
                'Success! Opened SSH and SCP to PoreMongo'
            )

    def disconnect(self, ssh=False):

        self.logger.debug(
            f'Attempting to disconnect from: {self.decompose_uri()}'
        )

        self.client.close()

        self.client, self.db, self.fast5 = None, None, None

        self.logger.info(
            f'Disconnected from: {self.decompose_uri()}'
        )

        if ssh:
            self.logger.info(
                'Attempting to close SSH and SCP'
            )
            self.close_ssh()
            self.close_scp()
            self.logger.info(
                'Closed SSH and SCP'
            )

    def open_scp(self):

        self.scp = SCPClient(
            self.ssh.get_transport()
        )

        return self.scp

    def close_scp(self):

        self.scp.close()

    def close_ssh(self):

        self.ssh.close()

    # TODO: Exceptions for SSH configuration file
    def open_ssh(self, config_file=None):

        if config_file:
            with open(config_file, 'r') as infile:
                config = json.load(infile)
                ssh_config = config["ssh"]
        else:
            ssh_config = self.config["ssh"]

        self.ssh = self.create_ssh_client(
            server=ssh_config["server"],
            port=ssh_config["port"],
            user=ssh_config["user"],
            password=ssh_config["password"]
        )

        return self.ssh

    @staticmethod
    def create_ssh_client(server, port, user, password):

        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(server, port, user, password)

        return client

    ##########################
    #     Fast5 Indexing     #
    ##########################

    # TODO: Config with paths and tags, comments
    def index(
        self,
        index_path: Path,
        recursive: bool = True,
        scan: bool = True,
        ncpu: int = 2,
        batch_size: int = 1000,
    ):

        """ Main access method to index Fast5 files into MongoDB

        :param index_path
            Path to to search for Fast5 files

        :param recursive
            Search for Fast5 recursively

        :param scan
            Scan and extract database model from Fast5,
            otherwise simply index model with file path only

        :param ncpu
            Number of processors to use, recommended for any
            reasonably large Fast5 collection. Default is to
            use multiprocessing inserts (ncpu = 2)

            Warning! Documents are inserted with a unique identifier
            field. As the batch insertion does not check whether a
            file path is already in the database, you can therefore
            insert the same documents multiple times

        :param batch_size
            Number of documents to insert in one process.


        """

        self.logger.info(
            f'Initiate indexing of files (.fast5) in: {index_path}'
        )

        self.logger.info(
            f'Collecting files, this may take some time...'
        )

        file_paths = self.files_from_path(
            path=str(index_path),
            extension=".fast5",
            recursive=recursive
        )   # Returns generator!

        file_paths = list(file_paths)  # Load the list into memory for chunking

        self.logger.info(
            f'Collected {len(file_paths)} Fast5 files from: {index_path}'
        )

        self._index_fast5(
            file_paths=file_paths,
            scan_file=scan,
            batch_size=batch_size,
            ncpu=ncpu
        )

    def _index_fast5(
        self,
        file_paths: list,
        scan_file: bool = True,
        ncpu: int = 1,
        batch_size: int = 1000
    ):

        total = 0
        batch_number = 0

        if ncpu > 1:

            self.logger.info(
                f'Parallel inserts enabled, processors: {ncpu}'
            )

            # Disconnect to initiate parallel connections
            if self.is_connected():

                self.disconnect()
                self.logger.info(
                    'Disconnected from database to initiate '
                    'parallel database connections'
                )

            self.logger.info(
                f'Chunking files into batches of size {batch_size}'
            )
            chunks = self._chunk_seq(
                file_paths, batch_size
            )  # in memory

            logger = self.logger # Local instance for callback

            def cbk(x):
                logger.info(x)

            pool = mp.Pool()
            for i, batch in enumerate(chunks):
                pool.apply_async(
                    _insert_doc,
                    args=(batch, self.uri, i, scan_file, ),
                    callback=cbk
                )   # Only static methods work, out-sourced functions to utils
            pool.close()
            pool.join()

            self.logger.info(
                f'Inserted {len(file_paths)} Fast5 files into database'
            )
            self.logger.info(
                f'Reconnecting to database on single connection.'
            )
            self.connect()

        else:
            # This is a batch wise insert on a generator:
            batch = []
            for file_path in file_paths:
                m = _get_doc(
                    file_path,
                    scan_file=scan_file,
                    to_mongo=False,
                    model=Fast5
                )
                batch.append(m)

                # At each batch size save batch to collection and clear batch
                if len(batch) == batch_size:
                    total = self._save_batch(
                        batch, batch_number, total, model=Fast5
                    )
                    batch = []
                    batch_number += 1

            # Save last batch
            if batch:
                self._save_batch(
                    batch, batch_number, total, model=Fast5,
                )

    def _save_batch(
        self,
        batch,
        batch_number,
        total,
        model=Fast5
    ):

        model.objects.insert(batch)
        total += len(batch)

        self.logger.info(
            f'Inserted {len(batch)} documents '
            f'(batch: {batch_number}, total: {total})'
        )

        return total

    ##########################
    #     Poremongo Tags     #
    ##########################

    def tag(self, tags, path_query=None, name_query=None, tag_query=None,
            raw_query=None, remove=False, recursive=True, not_in=False):

        """

        :param tags:
        :param path_query:
        :param name_query:
        :param tag_query:
        :param remove:
        :param recursive:
        :param not_in
        :param raw_query
        :return:
        """

        if isinstance(tags, str):
            tags = (tags,)

        self.logger.info(f'Updating tags to: {", ".join(tags)}')

        objects = self.query(
            model=Fast5,
            raw_query=raw_query,
            path_query=path_query,
            name_query=name_query,
            tag_query=tag_query,
            recursive=recursive,
            not_in=not_in)

        if remove:
            objects.update(pull_all__tags=tags)
        else:
            objects.update(add_to_set__tags=tags)

    ##########################
    #     DB Summaries       #
    ##########################

    @staticmethod
    def display_tags(self, limit: int = 100):

        pipe = [
            {"$match": {"tags": {"$not": {"$size": 0}}}},
            {"$unwind": "$tags"},
            {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
            {"$match": {"count": {"$gte": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": limit}
        ]

        tag_counts = list(
            Fast5.objects.aggregate(*pipe)
        )

        for x in tag_counts:
            print(
             f"{x['_id']:<10} = {x['count']:>10}"
            )

    #########################
    #   Cleaning DB + QC    #
    #########################

    # TODO: parse FASTQ for basecalled reads and attach Sequence model to Fast5

    def link_fastq(self, fastq):

        from pyfastaq import sequences

        fq_reader = sequences.file_reader(fastq)

        fq_stats = {}
        i = 0
        for seq in fq_reader:
            qual = self.average_quality(
                [ord(x) - 33 for x in seq.qual]
            )
            seq_info = seq.id.split(" ")

            fq_stats[seq_info[0]] = {'length': len(seq), 'quality': qual}
            i += 1
            if i == 5:
                break

        print(
            fq_stats.keys()
        )

        # Return only the Fast5 objects where the
        # nested reads have the parsed IDs
        fast5 = Fast5.objects(
            __raw__={
                'reads.id': {'$in': list(
                    fq_stats.keys()
                )}
            }
        )

        fq = Path(fastq)
        fq_path, fq_dir, fq_name = fq.absolute(), fq.parent.absolute(), fq.name

        for f5 in fast5:
            if len(f5.reads) > 1:
                print(f"Detected unsupported complementary"
                      f" strand, skipping Fast5: {fast5.name}.")
                continue

            for read in f5.reads:
                seq = Sequence(
                    id=None,
                    basecaller="albacore",
                    version="2.1",
                    path=fq_path,
                    dir=fq_dir,
                    name=fq_name,
                    quality=fq_stats[read.id]["quality"],
                    length=fq_stats[read.id]["length"]
                )

                read.sequences.append(
                    seq
                )  # Could have multiple basecalled sequences

                print(read.sequences)

    @staticmethod
    def average_quality(quals):

        """
        Receive the integer quality scores of a read and return
        the average quality for that read

        First convert Phred scores to probabilities,
        calculate average error probability and
        convert average back to Phred scale.

        https://gigabaseorgigabyte.wordpress.com/2017/06/26/
        averaging-basecall-quality-scores-the-right-way/
        """

        return -10 * math.log(sum([10 ** (q / -10) for q in quals]) / len(quals), 10)

    # SELECTION AND MAPPING METHODS

    def watch(
        self,
        path,
        callback=None,
        recursive=False,
        regexes=(".*\.fastq$",),
        **kwargs
    ):

        """Pomoxis async watchdog to index path for .fast5
        files, apply callback function to filepath on event detection.

        :param path:
        :param callback:
        :param index:
        :param recursive:
        :param sleep:
        :return:
        """

        if callback is None:
            callback = self.callback_upsert

        fw = FileWatcher()

        try:
            fw.watch_path(
                path=path,
                callback=callback,
                recursive=recursive,
                regexes=regexes,
                **kwargs,
            )
        except KeyboardInterrupt:
            self.logger.info('Path watcher interrupted by user. Exiting.')
            exit(1)
        except SystemError:
            self.logger.info('Path watcher interrupted by system. Exiting.')
            exit(1)

    def callback_upsert(self, fpath):
        """Callback for upsert of newly detected .fast5 file
        into database as Fast5 model.

        :param fpath:
        :return:
        """
        fast5 = _get_doc(fpath, scan_file=True)
        print(f"Fast5: {time.time()} name={str(fast5.name)}")

    def schedule_run(self, fast5, outdir="run_sim_1", scale=1.0, timeout=None):
        """Schedule a run extracted from sorted completion times
        for reads contained in Fast5 models. Scale adjusts the
        time intervals between reads. Use with group_runs to
        extract reads from the same sequencing run.

        Based on taeper - Michael Hall - https://github.com/mhall88/taeper

        :param fast5:
        :param sort:
        :param scale:
        :param outdir:
        :param timeout:
        :return:
        """
        # WIll double copy of Fast5 for 2d reads as of now TODO
        reads = [(read, f5) for f5 in fast5 for read in f5.reads]
        # Sort by ascending read completeion times first
        reads = sorted(reads, key=lambda x: x[0].end_time, reverse=False)

        read_end_times = [read[0].end_time for read in reads]
        # Compute difference between completion of reads
        time_delta = [0] + [delta/scale for delta in self._delta(read_end_times)]

        scheduler = BackgroundScheduler()
        start = time.time()  # For callback
        run = datetime.now()  # For scheduler
        for i, delay in enumerate(time_delta):
            run += timedelta(seconds=delay)
            scheduler.add_job(self.copy_read, 'date', run_date=run,
                              kwargs={'read': reads[i][0], 'start': start,
                                      'fast5': reads[i][1], 'outdir': outdir})
        scheduler.start()
        if not timeout:
            print(f"Press Ctrl+{'Break' if os.name == 'nt' else 'C'} to exit")
            try:
                # Simulate application activity (which keeps the main thread alive).
                while True:
                    time.sleep(2)
            except (KeyboardInterrupt, SystemExit):
                scheduler.shutdown()
        else:
            time.sleep(timeout)
            scheduler.shutdown()

    def copy_read(self, read, start, fast5, outdir):

        os.makedirs(os.path.abspath(outdir), exist_ok=True)

        shutil.copy(fast5.path, os.path.abspath(outdir))

        self._print_read(read.id, start)

    @staticmethod
    def _print_read(name, start):

        now = time.time()
        elapsed = round(float(now - start), 4)
        print(f"Read: {time.ctime(now)} elapsed={elapsed} name={name}")

        return now

    @staticmethod
    def _delta(times):

        return [times[n] - times[n - 1] for n in range(1, len(times))]

    @staticmethod
    def group_runs(fast5):

        pipeline = [{"$group": {"_id": "$exp_start_time",
                                "fast5": {"$push": "$_id"}}}]
        run_groups = list(fast5.aggregate(*pipeline, allowDiskUse=True))

        runs = {}
        for run in run_groups:
            timestamp = int(run["_id"])
            entry = {"run": datetime.fromtimestamp(timestamp),
                     "fast5": run["fast5"]}
            runs[timestamp] = entry
        print(f"Extracted {len(runs)} {'run' if len(runs) == 1 else 'runs'}.")

        return runs

    @staticmethod
    def sample(file_objects, limit=3, tags=None, proportion=None, unique=False, include_tags=None,
               exclude_name=None, return_documents=True):

        """ Add query to a queryset (file_objects) to sample a limited number of file objects;
        these can be sampled proportionally by tags. """

        if isinstance(tags, str):
            tags = [tags]

        if isinstance(include_tags, str):
            include_tags = [include_tags]

        if tags:

            if exclude_name:
                query_pipeline = [{"$match": {"name": {"$nin": exclude_name}}}]
            else:
                query_pipeline = []

            if include_tags:
                query_pipeline += [{"$match": {"tags": {"$all": include_tags}}}]

            # Random sample across given tags:
            if not proportion:
                print(f"Tags specified, but no proportions, sample {limit} Fast5 from all (&) tags: {tags}")
                query_pipeline += [
                    {"$match": {"tags": {"$all": tags}}},
                    {"$sample": {"size": limit}}
                ]
                results = list(file_objects.aggregate(*query_pipeline, allowDiskUse=True))

            # Equal size of random sample for each tag:
            elif proportion == "equal":
                print(f"Tags specified, equal proportions, sample {int(limit/len(tags))} Fast5 for each tag: {tags}")
                results = []
                for tag in tags:
                    query_pipeline += [
                        {"$match": {"tags": {"$in": [tag]}}},
                        {"$sample": {"size": int(limit/len(tags))}}
                    ]
                    results += list(file_objects.aggregate(*query_pipeline, allowDiskUse=True))
            else:

                if not len(proportion) == len(tags):
                    raise ValueError("List of proportions must be the same length as list of tags.")
                if not sum(proportion) == 1:
                    raise ValueError("List of proportions must sum to 1.")

                print(f"Tags specified, list of proportions, sample tags -- "
                      f"{', '.join([': '.join([tag, str(int(prop*limit))]) for tag, prop in zip(tags, proportion)])}")

                results = []
                for i in range(len(tags)):
                    lim = int(limit * proportion[i])
                    query_pipeline += [
                        {"$match": {"tags": {"$in": [tags[i]]}}},
                        {"$sample": {"size": lim}}
                    ]
                    results += list(file_objects.aggregate(*query_pipeline, allowDiskUse=True))

        else:
            print(f"No tags specified, sample {limit} files over given file objects")
            query_pipeline = [
                {"$sample": {"size": limit}}
            ]

            results = list(file_objects.aggregate(*query_pipeline, allowDiskUse=True))

        if unique:
            results = list(set(results))

        if return_documents:
            results = [Fast5(**result) for result in results]

        return results

    @staticmethod
    def to_csv(file_objects, out_file, labels=None, sep=","):

        print(f"Writing file paths of Fast5 documents to {out_file}.")

        data = {"paths": [obj.path for obj in file_objects]}

        if labels:
            data.update({"labels": labels})

        pandas.DataFrame(data).to_csv(out_file, header=None, index=None, sep=sep)

    def copy(self, file_objects, outdir, exist_ok=True, symlink=False, iterate=False,
             ncpu=1, chunk_size=100, prefixes=None):

        """ Copy or symlink into output directory, use either generator (memory efficient, ncpu = 1) or
        list for memory dependent progbar (ncpu = 1) or multi-processing (speedup, ncpu > 1)"""

        # If files are stored on remote server, copy the files using Paramiko and SCP

        # Do this as iterator (ncpu = 1, if iterate) or in memory (ncpu > 1, ncpu = 1 if not iterate, has progbar)

        if ncpu == 1:

            os.makedirs(outdir, exist_ok=exist_ok)

            if iterate:
                self.link_files(file_objects, outdir=outdir, pbar=None, symlink=symlink,
                                scp=self.scp, prefixes=prefixes)
            else:
                file_objects = list(file_objects)
                with tqdm.tqdm(total=len(file_objects)) as pbar:
                    self.link_files(file_objects, outdir=outdir, pbar=pbar, symlink=symlink,
                                    scp=self.scp, prefixes=prefixes)
        else:

            if self.scp:
                raise ValueError("You are trying to call the copy method with multiprocessing options, "
                                 "while connected to remote server via SHH. This is currently not "
                                 "supported by PoreMongo.")

            os.makedirs(outdir, exist_ok=exist_ok)

            # Multiprocessing copy of file chunks, in memory:
            file_objects = list(file_objects)

            file_object_chunks = self._chunk_seq(file_objects, chunk_size)
            nb_chunks = len(file_object_chunks)

            if prefixes:
                prefix_chunks = self._chunk_seq(prefixes, chunk_size)
            else:
                prefix_chunks = [None for _ in range(nb_chunks)]

            print(f"Linking file chunks across processors (number of chunks = {nb_chunks}, ncpu = {ncpu})...")

            # Does not work for multiprocessing

            pool = mp.Pool(processes=ncpu)
            for i in range(nb_chunks):
                pool.apply_async(self.link_files, args=(file_object_chunks[i], outdir, None,
                                                        symlink, self.scp, prefix_chunks[i]))
            pool.close()
            pool.join()

    @staticmethod
    def _chunk_seq(seq, size):

        # Generator
        return [seq[pos:pos + size] for pos in range(0, len(seq), size)]

    @staticmethod
    def link_files(file_objects, outdir: str, symlink: bool = False, pbar=None, scp=None, prefixes=None):

        for i, obj in enumerate(file_objects):

            if scp is not None:

                if prefixes:
                    prefix = prefixes[i]
                else:
                    prefix = None

                obj.get(scp, out_dir=outdir, prefix=prefix)

            else:
                # For results from aggregation (dicts)
                if isinstance(obj, dict):
                    obj_path = obj["path"]
                    obj_name = obj["name"]
                else:
                    obj_path = obj.path
                    obj_name = obj.name

                if prefixes:
                    obj_name = prefixes[i] + "_" + obj_name

                if symlink:
                    # If not copy, symlink:
                    target_link = os.path.join(outdir, obj_name)
                    os.symlink(obj_path, target_link)
                else:
                    # Copy files to target directory
                    shutil.copy(obj_path, outdir)

            if pbar:
                pbar.update(1)


    def files_from_cache(self):

        # Cache is run summary file:

        # Cache is generated when doing a path search:

        pass

    @staticmethod
    def files_from_path(path: str, extension: str, recursive: bool):

        if not recursive:
            # Yielding files
            for file in os.listdir(path):
                if file.endswith(extension):
                    yield os.path.abspath(os.path.join(path, file))
        else:
            # Recursive search should be slightly faster in 3.6
            # is always a generator:
            for p, d, f in os.walk(path):
                for file in f:
                    if file.endswith(extension):
                        yield os.path.abspath(os.path.join(p, file))


    def comment(self, comments, path_query=None, name_query=None, tag_query=None, remove=False, recursive=True):

        """ Add comments to all files with extension tag_path if and only if they are already indexed in the DB.
        Default recursive (for all Fast5 where tag_path in file_path) or optional non-recursive search
        (for all Fast5 where tag_path is parent_path) and printing summary.

        :param comments:
        :param path_query:
        :param name_query:
        :param tag_query:
        :param remove:
        :param recursive:
        :return:

        """

        if isinstance(comments, str):
            comments = (comments,)
        if isinstance(comments, str):
            comments = (comments,)
            if isinstance(comments, str):
                comments = (comments,)

        if not self._one_active_param(path_query, name_query, tag_query):
            raise ValueError("Tags can only be attached by one (str) "
                             "or multiple (list) attributes of either: path, name or tag")

        objects = self.query(model=Fast5, path_query=path_query, name_query=name_query,
                             tag_query=tag_query, recursive=recursive)

        if remove:
            objects.update(pull_all__comments=comments)
        else:
            objects.update(add_to_set__comments=comments)

    @staticmethod
    def _one_active_param(path_query, name_query, tag_query):

        return sum([True for param in [path_query, name_query, tag_query] if param is not None]) == 1

    # DB METHODS

    @staticmethod
    def filter(queryset, limit: int = None, shuffle: bool = False, unique: bool = True, length: int = None):

        """ Filter where query sets are now living in memory """

        query_results = list(queryset)  # Lives happily ever after in memory.

        if unique:
            query_results = list(set(query_results))

        if shuffle:
            random.shuffle(query_results)

        if limit:
            query_results = query_results[:limit]

        return query_results

    def query(self, raw_query=None, path_query: str or list = None, tag_query: str or list = None,
              name_query: str or list = None, query_logic: str = "AND", model: Fast5 or Read or Sequence=Fast5,
              abspath: bool = False, recursive: bool = True, not_in: bool = False):

        """ API for querying file models using logic chains on path, tag or name queries. MongoEngine queries to path,
        tag and names (Q) are chained by bitwise operator logic (query_logic) and path, tag and name queries can
        be also be chained with each other if at least two parameters given (all same operator for now = query_logic).

        Single queries can also use the context_manager methods on the Fast5 model class in a connected DB,
        i.e. Fast5.query_name(name="test"), Fast5.query_path(path="test_path"), Fast5.query_tags(tags="tag_1").
        """

        # TODO implement nested lists as query objects and nested logic chains?

        if raw_query:
            return model.objects(__raw__=raw_query)

        if isinstance(path_query, str):
            path_query = [path_query, ]
        if isinstance(tag_query, str):
            tag_query = [tag_query, ]
        if isinstance(name_query, str):
            name_query = [name_query, ]

        # Path filter should ask for absolute path by default:
        if abspath and path_query:
            path_query = [os.path.abspath(pq) for pq in path_query]

        # Path filter for selection:
        if path_query:
            path_queries = self.get_path_query(path_query, recursive, not_in)
        else:
            path_queries = list()

        if name_query:
            name_queries = self.get_name_query(name_query, not_in)
        else:
            name_queries = list()

        if tag_query:
            tag_queries = self.get_tag_query(tag_query, not_in)
        else:
            tag_queries = list()

        queries = path_queries + name_queries + tag_queries

        if not queries:
            # If there are no queries, return all models:
            return model.objects

        # Chain all queries (within and between queries) with the same bitwise operator | or &
        query = self.chain_logic(queries, query_logic)

        return model.objects(query)

    @staticmethod
    def get_tag_query(tag_query, not_in):

        if not_in:
            return []  # TODO
        else:
            return [Q(tags=tq) for tq in tag_query]

    @staticmethod
    def get_name_query(name_query, not_in):

        if not_in:
            return [Q(__raw__={"name": {'$regex': '^((?!{string}).)*$'.format(string=nq)}})
                    for nq in name_query]  # case sensitive regex (not contains)
        else:
            return [Q(name__contains=nq) for nq in name_query]

    # TODO: Abspath - on Windows, UNIX

    @staticmethod
    def get_path_query(path_query, recursive, not_in):

        if recursive:
            if not_in:
                return [Q(__raw__={"path": {'$regex': '^((?!{string}).)*$'.format(string=pq)}})
                        for pq in path_query]  # case sensitive regex (not contains)
            else:
                return [Q(path__contains=pq) for pq in path_query]
        else:
            return [Q(dir__exact=pq) for pq in path_query]

    @staticmethod
    def chain_logic(iterable, logic):
        if logic in ("OR", "or", "|"):
            chained = reduce(or_, iterable)
        elif logic in ("AND", "and", "&"):
            chained = reduce(and_, iterable)
        else:
            raise ValueError("Logic parameter must be one of (AND, and, &) or (OR, or, |).")

        return chained

    # Small helpers


    def decompose_uri(self):

        if "localhost" not in self.uri:
            user_split = self.uri.replace("mongodb://", "").split("@")
            return "mongodb://" + user_split.pop(0).split(":")[0] + \
                   "@" + "@".join(user_split)
        else:
            return self.uri





