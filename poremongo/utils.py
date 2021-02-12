import os
import sys
import shlex
import subprocess
import json as js

from pathlib import Path
from poremongo.poremodels import Read
from mongoengine.queryset import QuerySet


def cli_input(json_in: str or None):

    if json_in:
        if json_in == "-":
            # STDIN JSON
            docs = []
            for entry in sys.stdin:
                doc = js.loads(entry.rstrip())
                docs.append(doc)

            read_objects = QuerySet(document=Read, collection='fast5')
            read_objects.from_json(js.dumps(docs))
        else:
            # FILE JSON
            with Path(json_in).open('r') as infile:
                read_objects = QuerySet(document=Read, collection='fast5')
                read_objects.from_json(infile)
    else:
        # Read objects in DB
        read_objects = Read.objects

    return read_objects


def cli_output(read_objects, json_out: str or None, display: bool, pretty: bool):

    if display:
        for o in read_objects:
            o.pretty_print = pretty
            print(o)

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



