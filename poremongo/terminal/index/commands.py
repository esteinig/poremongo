import click

from pathlib import Path
from functools import partial
from poremongo.poremongo import PoreMongo

# Monkey patching to show all default options
click.option = partial(click.option, show_default=True)


@click.command()
@click.option(
    '--uri', '-u', type=str, default='local',
    help='MongoDB URI, >local< to start a local scratch DB,'
         'or connect via URI: mongodb://user:pwd@address:port/dbname'
)
@click.option(
    '--index_path', '-p', type=Path, required=True,
    help='Directory containing Fast5.'
)
@click.option(
    '--config', '-c', type=Path, default=None,
    help='Path to JSON config file for MongoDB connection.'
)
@click.option(
    '--tags', '-t', type=str, required=True,
    help='Comma separated string of tags (labels) to attach to Fast5 in DB'
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
@click.option(
    '--mongod', '-m', is_flag=True,
    help='Start local MongoDB database in background process.'
)
@click.option(
    '--port', default=27017,
    help='Port for connecting to localhost MongoDB'
)
def index(
    uri, config, index_path, index_file, tags,
    recursive, batch_size, ncpu, mongod, port
):

    """ Index a path to Fast5 files and tag documents in the DB. """

    if uri == 'local':
        uri = f'mongodb://localhost:{port}/poremongo'

    tags = [t.strip() for t in tags.split(',')]

    if not tags:
        raise ValueError('You must specify tags (labels) for indexing Fast5')

    pongo = PoreMongo(
        config=config if config else dict(),
        uri=uri if uri else None
    )

    if pongo.local and mongod:
        pongo.start_mongodb()

    pongo.connect()

    pongo.index(
        index_path=index_path,
        index_file=None,
        recursive=recursive,
        scan=True,
        batch_size=batch_size,
        ncpu=ncpu
    )

    pongo.tag(
        path_query=index_path,
        tags=tags,
        recursive=recursive
    )

    pongo.disconnect()

    if pongo.local and mongod:
        pongo.terminate_mongodb()
