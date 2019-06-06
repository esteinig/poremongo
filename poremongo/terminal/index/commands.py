import click
from pathlib import Path

from poremongo.poremongo import PoreMongo

from functools import partial

# Monkey patching to show all default options
click.option = partial(click.option, show_default=True)

@click.command()
@click.option(
    '--uri', '-u', type=str, required=True,
    help='MongoDB URI, >local< to start a local scratch DB,'
         'or connect via URI: mongodb://user:pwd@address:port/dbname'
)
@click.option(
    '--tags', '-t', type=str, required=True,
    help='Comma separated string of tags (labels) to attach to Fast5 in DB'
)
@click.option(
    '--fast5', '-f', type=str, required=True,
    help='Directory containing Fast5.'
)
@click.option(
    '--recursive', '-r', is_flag=True,
    help='Recursive search through directory.'
)
@click.option(
    '--batch_size', '-b', type=int, default=500,
    help='Batch size for parallel multi-inserts into DB'
)
@click.option(
    '--ncpu', '-n', type=int, default=2,
    help='Number of processors for parallel multi-inserts into DB.'
)
def index(uri, fast5, tags, recursive, batch_size, ncpu):

    if uri == 'local':
        uri = 'mongodb://localhost:27017/poremongo'

    tags = tags.split(',')

    if not tags:
        raise ValueError('You must specify tags (labels) for indexing Fast5')

    pongo = PoreMongo(uri=uri)

    pongo.connect()

    pongo.index(
        index_path=fast5,
        recursive=recursive,
        scan=True,
        batch_size=batch_size,
        ncpu=ncpu
    )

    pongo.tag(
        path_query=str(fast5),
        tags=tags,
        recursive=recursive
    )

