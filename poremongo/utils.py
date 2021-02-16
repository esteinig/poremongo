import os
import sys
import shlex
import uuid
import subprocess
import json as js

from pathlib import Path
from poremongo.poremodels import Read
from pymongo import MongoClient

from ont_fast5_api.fast5_interface import get_fast5_file


def cli_output(read_objects, json_out: str or None, detail: bool):

    if json_out:
        if isinstance(read_objects, list):
            data_dict = [js.loads(o.to_json()) for o in read_objects]
        else:
            data_dict = js.loads(
                read_objects.to_json()
            )

        if json_out == "-":
            for o in data_dict:
                print(js.dumps(o))
        else:
            with Path(json_out).open('w') as outfile:
                js.dump(data_dict, outfile)
    else:
        for o in read_objects:
            o.pretty_print = not detail
            print(o)


def run_cmd(cmd, callback=None, watch=False, shell=False):

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
        if shell:
            return subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                shell=True,
                preexec_fn=os.setsid
            )
        else:
            proc = subprocess.Popen(
                shlex.split(cmd),
                stdout=subprocess.PIPE
            )
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


def parse_read_documents(file: Path, tags: [str], store_signal: bool = False, add_signal_info: bool = False):

    reads = []
    with get_fast5_file(str(file), mode="r") as f5:
        for read in f5.get_reads():
            unique_identifier = uuid.uuid4()
            fast5_path = file.absolute()
            read = Read(
                fast5=str(fast5_path),
                uuid=str(unique_identifier),
                tags=tags,
                read_id=read.read_id,
                signal_data=read.get_signal(
                    start=None, end=None, scale=False
                ) if store_signal else list(),
                signal_data_length=len(
                    read.get_signal(start=None, end=None, scale=False)
                ) if add_signal_info else 0
            )
            reads.append(read)

    return reads


def multi_insert(
    file, uri, tags: list = None, store_signal: bool = False, add_signal_info: bool = False, thread_number: int = 1
):

    """
    For multiprocessing use MongoClient directly to spawn new connections to Fast5 collection
    """

    reads = parse_read_documents(file=file, tags=tags, store_signal=store_signal, add_signal_info=add_signal_info)
    print(f"From inside process: {uri}")

    myclient = MongoClient("mongodb://localhost:27017/")
    mydb = myclient["poremongo"]
    mycol = mydb["fast5"]
    print(mycol)
    print([r.to_json() for r in reads])
    x = mycol.insert_many([r.to_json() for r in reads])
    print(x)





