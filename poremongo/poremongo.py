""" Minimal extension of PoreDB by Nick Loman et al. for Fast5 management in MongoDB """

import os
import tqdm
import json
import random
import shutil
import pymongo

from textwrap import dedent
from functools import reduce
import multiprocessing as mp
from operator import or_, and_
from colorama import Fore, Style

from mongoengine import connect
from mongoengine.queryset.visitor import Q
from mongoengine.errors import NotUniqueError

from poremongo.models import Fast5


class PoreMongo:

    """ API for PoreMongo DB: includes main models: minimal, standard, signal, sequence (increasing size of DB) """

    def __init__(self, uri, config=None):

        self.uri = uri

        if config:
            self._parse_config(config)

        self.verbose = True

        self.client = None
        self.db = None
        self.fast5 = None

    def _parse_config(self, config):

        with open(config, "r") as config_file:
            config_dict = json.load(config_file)
            self.uri = config_dict["uri"]

    def is_connected(self):

        return True if self.client else False

    def connect(self, verbose=True, is_mock=False):

        self.client = connect(host=self.uri, is_mock=is_mock)

        if verbose:
            self.print_connected_message()

        # Database: poremongo
        self.db = self.client.db

        # Collection: fast5
        self.fast5 = self.db.fast5

    def disconnect(self):

        self.client.close()

        self.client, self.db, self.fast5 = None, None, None

    ##########################
    #   Model: Operations    #
    ##########################

    # Updates: defined in model methods
    # Queryset filters: defined in model methods

    ##########################
    #   Fast5: Index + Tags  #
    ##########################

    # Connect with SSH to rsync files over to local

    # SELECTION AND MAPPING METHODS

    def simulate(self, api="taeper"):

        """ Simulations of sequencing runs with Taeper and read until with Promoxis integrated into PoreMongo """

        pass

    def watch(self, thread=False, extension=".fast5", promoxis_async=True):

        """ Watch a directory indefinitely and add Fast5 to database as soon as they are generated,
         use the async feedback loop callback in Promoxis """

        pass

    @staticmethod
    def sample(file_objects, limit=3, tags=None, proportion=None, unique=False):

        """ Add query to a queryset (file_objects) to sample a limited number of file objects;
        these can be sampled proportionally by tags. """

        if tags:

            # Random sample across given tags:
            if not proportion:
                print(f"Tags specified, but no proportions, sample {limit} Fast5 across tags: {', '.join(tags)}.")
                query_pipeline = [
                    {"$match": {"tags": {"$in": tags}}},
                    {"$sample": {"size": limit}}
                ]
                results = list(file_objects.aggregate(*query_pipeline, allowDiskUse=True))

            # Equal size of random sample for each tag:
            elif proportion == "equal":
                print(f"Tags specified, equal proportions, sample {limit} Fast5 for each tag: {', '.join(tags)}.")
                results = []
                for tag in tags:
                    query_pipeline = [
                        {"$match": {"tags": {"$in": [tag]}}},
                        {"$sample": {"size": limit}}
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
                    query_pipeline = [
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

        # List of dictionaries:
        print("Results:", len(results))

        for r in results:
            print(r["name"], r["tags"])

        return results

    def copy(self, file_objects, outdir, exist_ok=True, symlink=False, iterate=True, ncpu=1, mp_chunk=100):

        """ Copy or symlink into output directory, use either generator (memory efficient, ncpu = 1) or
        list for memory dependent progbar (ncpu = 1) or multi-processing (speedup, ncpu > 1)"""

        os.makedirs(outdir, exist_ok=exist_ok)

        # Do this as iterator (ncpu = 1, if iterate) or in memory (ncpu > 1, ncpu = 1 if not iterate, has progbar)

        if ncpu == 1:
            if iterate:
                self.link_files(file_objects, outdir=outdir, pbar=None, symlink=symlink)
            else:
                file_objects = list(file_objects)
                with tqdm.tqdm(total=len(file_objects)) as pbar:
                    self.link_files(file_objects, outdir=outdir, pbar=pbar, symlink=symlink)
        else:
            # Multiprocessing copy of file chunks, in memory:
            file_objects = list(file_objects)

            file_object_chunks = self._chunk_seq(file_objects, mp_chunk)
            nb_chunks = len(file_object_chunks)

            print(f"Linking file chunks across processors (number of chunks = {nb_chunks}, ncpu = {ncpu})...")

            # def cbk(linked):
            #     pass

            pool = mp.Pool(processes=ncpu)
            for i in range(nb_chunks):
                pool.apply_async(self.link_files, args=(file_object_chunks[i], outdir, None, symlink,))
            pool.close()
            pool.join()

    @staticmethod
    def _chunk_seq(seq, size):

        # Generator
        return [seq[pos:pos + size] for pos in range(0, len(seq), size)]

    @staticmethod
    def link_files(file_objects, outdir: str, symlink: bool = True, pbar=None):

        for obj in file_objects:

            # For results from aggregation (dicts)
            if isinstance(obj, dict):
                obj_path = obj["path"]
                obj_name = obj["name"]
            else:
                obj_path = obj.path
                obj_name = obj.name

            if symlink:
                # If not copy, symlink:
                target_link = os.path.join(outdir, obj_name)
                os.symlink(obj_path, target_link)
            else:
                # Copy files to target directory
                shutil.copy(obj_path, outdir)

            if pbar:
                pbar.update(1)

    # FILE METHODS
    # TODO: Config with paths and tags, comments
    def index(self, index_path: str, recursive: bool = True, scan: bool = True, insert: bool = False, ncpu: int = 1,
              batch_size: int = 1000):

        """ Index all files with extension in unique index_path and assign primary index_id and save in
        corresponding collection_id of DB. Optional recursive search, printing summary and tagging.

        :param index_path:          str            index_path to index
        :param recursive:           bool           recursive search in directory tree
        :param scan                 bool
        :param insert
        :param ncpu
        :param batch_size

        """

        if self.verbose:
            self.print_index_message(index_path, ".fast5", insert)

        # Can we keep this a generator for really large file collections?
        file_paths = self.files_from_path(path=index_path, extension=".fast5", recursive=recursive)

        self.index_fast5(file_paths=file_paths, scan_file=scan, insert=insert, batch_size=batch_size, ncpu=ncpu)

    def index_fast5(self, file_paths, scan_file=False, insert=False, ncpu=1, batch_size=1000):

        """ Index Fast5 files with supported extensions (model schemes for DB) in batches """

        total = 0
        batch_number = 0

        # TODO: Check generators / lists:

        if ncpu > 1:

            self.disconnect()
            print(f"Multiprocessing enabled: {Fore.RED}Disconnected{Style.RESET_ALL} from current MongoClient\n")

            print(f"Parsing file paths for batching and parallel inserts to PoreMongo...")

            chunks = self._chunk_seq(list(file_paths), batch_size)  # in memory

            self.print_ncpu_insert_message(len(chunks), batch_size, ncpu)

            def cbk(inserted):

                print(f"Inserted {Fore.YELLOW}{inserted[0]}{Style.RESET_ALL} documents (Fast5) "
                      f"(batch {Fore.GREEN}{inserted[1]}{Style.RESET_ALL}) ")

            pool = mp.Pool(processes=ncpu)
            for i, chunk in enumerate(chunks):
                pool.apply_async(self._insert_doc, (chunk, i, scan_file,), callback=cbk)

            pool.close()
            pool.join()

            self.connect(verbose=False)
            print(f"\nMultiprocessing completed: {Fore.GREEN}reconnected{Style.RESET_ALL} PoreMongo with MongoClient")

        else:
            # This is a batch wise insert on a generator:
            batch = []
            for file_path in file_paths:
                m = self._get_doc(file_path, scan_file=scan_file, to_mongo=False, model=Fast5)
                batch.append(m)

                # at each batch size save batch to collection and clear batch
                if len(batch) == batch_size:
                    total = self._save_batch(batch, batch_number, total,
                                             model=Fast5, insert=insert)
                    batch = []
                    batch_number += 1

            # save last batch
            self._save_batch(batch, batch_number, total,
                             model=Fast5, insert=insert)

    @staticmethod
    def _get_doc(file_path, scan_file=True, to_mongo=False, model=Fast5):
        """ Construct Fast5 document, optionally scan file for content and transform to dict
        for insert_multiple in PyMongo  (multiprocessing compatible inserts to MongoDB
        """
        fname = os.path.basename(file_path)
        fast5 = model(name=fname, path=file_path, dir=os.path.dirname(file_path))

        if scan_file:
            fast5.scan_file(update=False)

        if to_mongo:
            return fast5.to_mongo()

        return fast5

    def _insert_doc(self, chunk, i, scan_file):

        """ For multiprocessing use MongoClient
        directly to spawn new connections to Fast5 collection

        :param chunk:

        """

        batch = [self._get_doc(file_path, scan_file=scan_file, to_mongo=True, model=Fast5) for file_path in chunk]

        client = pymongo.MongoClient(self.uri)
        collection = client.poremongo.fast5
        collection.insert_many(batch)

        return len(batch), i

    def _save_batch(self, batch, batch_number, total, model=Fast5, insert=False):

        if insert:
            model.objects.insert(batch)
            if self.verbose:
                total += len(batch)
                self.print_insert_message(len(batch), batch_number, model, total)
        else:
            new = 0
            for m in batch:
                try:
                    m.save()
                    new += 1
                except NotUniqueError:
                    pass
                total += 1
            if self.verbose:
                self.print_insert_message(new, batch_number, model, total)

        return total

    def files_from_cache(self):

        # Cache is run summary file:

        # Cache is generated when doing a path search:

        pass

    @staticmethod
    def files_from_path(path, extension, recursive):

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

    # Database methods paths for tags and comments

    def tag(self, tags, path_query=None, name_query=None, tag_query=None, remove=False, recursive=True):

        """ Add tags to all files with extension tag_path if and only if they are already indexed in the DB.
        Default recursive (for all Fast5 where tag_path in file_path) or optional non-recursive search
        (for all Fast5 where tag_path is parent_path) and printing summary.

        :param tags:
        :param path_query:
        :param name_query:
        :param tag_query:
        :param remove:
        :param recursive:
        :return:
        """

        if isinstance(tags, str):
            tags = (tags,)

        if not self._one_active_param(path_query, name_query, tag_query):
            raise ValueError("Tags can only be attached by one (str) "
                             "or multiple (list) attributes of either: path, name or tag")

        objects = self.query(model=Fast5, path_query=path_query, name_query=name_query,
                             tag_query=tag_query, recursive=recursive)

        if self.verbose:
            print("Updating tags: {}".format(tags))

        if remove:
            objects.update(pull_all__tags=tags)
        else:
            objects.update(add_to_set__tags=tags)

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
              name_query: str or list = None, query_logic: str = "AND", model=Fast5,
              abspath: bool = False, recursive: bool = True, not_in: bool = False):

        """ API for querying file models using logic chains on path, tag or name queries. MongoEngine queries to path,
        tag and names (Q) are chained by bitwise operator logic (query_logic) and path, tag and name queries can
        be also be chained with each other if at least two parameters given (all same operator for now = query_logic).

        Single queries can also use the context_manager methods on the Fast5 model class in a connected DB,
        i.e. Fast5.query_name(name="test"), Fast5.query_path(path="test_path"), Fast5.query_tags(tags="tag_1").
        """

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
            if recursive:
                if not_in:
                    path_queries = [Q(__raw__={"path": {'$regex': '^((?!{string}).)*$'.format(string=pq)}})
                                    for pq in path_query]  # case sensitive regex (not contains)
                else:
                    path_queries = [Q(path__contains=pq) for pq in path_query]
            else:
                path_queries = [Q(dir__exact=pq) for pq in path_query]
        else:
            path_queries = list()

        if name_query:
            if not_in:
                name_queries = [Q(__raw__={"name": {'$regex': '^((?!{string}).)*$'.format(string=nq)}})
                                for nq in name_query]  # case sensitive regex (not contains)
            else:
                name_queries = [Q(name__contains=nq) for nq in name_query]
        else:
            name_queries = list()

        if tag_query:
            tag_queries = [Q(tags=tq) for tq in tag_query]
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
    def chain_logic(iterable, logic):
        if logic in ("OR", "or", "|"):
            chained = reduce(or_, iterable)
        elif logic in ("AND", "and", "&"):
            chained = reduce(and_, iterable)
        else:
            raise ValueError("Logic parameter must be one of (AND, and, &) or (OR, or, |).")

        return chained

    ###################
    # Message Methods #
    ###################

    @staticmethod
    def print_ncpu_insert_message(nb_chunks, batch_size, ncpu):

        print(dedent(f"""
        Starting database inserts with:

        - processors:       {Fore.YELLOW}{ncpu}{Style.RESET_ALL}        
        - total batches:    {Fore.YELLOW}{nb_chunks}{Style.RESET_ALL}   
        - files per batch:  {Fore.YELLOW}{batch_size}{Style.RESET_ALL}
        """))

    @staticmethod
    def print_insert_message(batch_length, batch_number, model, total):

        print(f"Inserted {Fore.GREEN}{batch_length}{Style.RESET_ALL} / "
              f"{Fore.YELLOW}{str(total).ljust(8)}{Style.RESET_ALL} files into"
              f" {Fore.YELLOW}{model.__name__}{Style.RESET_ALL} collection "
              f"(batch {Fore.GREEN}{batch_number}{Style.RESET_ALL})")

    @staticmethod
    def print_index_message(index_path, extension, insert):

        if insert:
            print(dedent(f"""
            {Fore.RED}======================================================{Style.RESET_ALL}
                            {Fore.YELLOW}INSERT is activated:{Style.RESET_ALL}
              Throws error, if any file path in collection of DB.
            {Fore.RED}======================================================{Style.RESET_ALL}
            """))

        print(dedent(f"""
        Collecting and indexing files ({extension}) in:

        {Fore.YELLOW}{index_path}{Style.RESET_ALL}

        This might take a while... how about a cup of coffee?


                            )  (
                          (   ) )     
                           ) ( (              
                         _________       
                      .-'---------| 
                     ( C|/\/\/\/\/| 
                      '-./\/\/\/\/|
                        '_________'             
                         '-------'  

        """))

    def print_connected_message(self):

        print(dedent(f"""
        {Fore.YELLOW}PoreMongo connected{Style.RESET_ALL}
        ===========================

        {Fore.YELLOW}{Fore.GREEN}{self.decompose_uri(self.uri)}{Style.RESET_ALL}
        """))

    @staticmethod
    def decompose_uri(uri):

        user_split = uri.replace("mongodb://", "").split("@")

        return "mongodb://" + user_split.pop(0).split(":")[0] + "@" + "@".join(user_split)






