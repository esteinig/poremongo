import os
import sys
import shlex
import subprocess
import pymongo

from pathlib import Path
from poremongo.models import Fast5


def run_cmd(cmd, callback=None, watch=False):

    """Runs the given command and gathers the output.

    If a callback is provided, then the output is sent to it, otherwise it
    is just returned.

    Optionally, the output of the command can be "watched" and whenever new
    output is detected, it will be sent to the given `callback`.

    Returns:
        A string containing the output of the command, or None if a `callback`
        was given.
    Raises:
        RuntimeError: When `index` is True, but no callback is given.

    """
    if watch and not callback:
        raise RuntimeError(
            'You must provide a callback when watching a process.'
        )

    output = None
    try:
        proc = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE)

        if watch:
            while proc.poll() is None:
                line = proc.stdout.readline()
                if line != "":
                    callback(line)

            # Sometimes the process exits before we have all of the output, so
            # we need to gather the remainder of the output.
            remainder = proc.communicate()[0]
            if remainder:
                callback(remainder)
        else:
            output = proc.communicate()[0]
    except:
        err = str(sys.exc_info()[1]) + "\n"
        output = err

    if callback and output is not None:
        return callback(output)

    return output


# Multiprocessing static functions for apply_async

def _insert_doc(
        chunk, uri, i=1, scan_file=True
    ):

    """
    For multiprocessing use MongoClient directly to spawn
    new connections to Fast5 collection

    :param chunk
        list of

    """

    db = Path(uri).stem

    batch = [
        _get_doc(
            file_path, scan_file=scan_file, to_mongo=True, model=Fast5
        )
        for file_path in chunk
    ]

    client = pymongo.MongoClient(uri)
    collection = client[db].fast5
    collection.insert_many(batch)

    client.close()  # ! Important, will otherwise refuse more connections

    return len(chunk), i  # Returns number of Fast5, index of batch


def _get_doc(file_path, scan_file=True, to_mongo=False, model=Fast5):

    """ Construct Fast5 document for MongoDB

    Optionally scan file for content and transform to dict
    for multiprocessing compatible inserts to MongoDB.

    """

    fname = os.path.basename(file_path)
    fast5 = model(
        name=fname, path=file_path, dir=os.path.dirname(file_path)
    )

    if scan_file:
        fast5.scan_file(update=False)

    if to_mongo:
        return fast5.to_mongo()

    return fast5


