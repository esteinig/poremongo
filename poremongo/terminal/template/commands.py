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
    '--config', '-c', type=Path, default=None,
    help='Path to JSON cofnig file for MongoDB connection.'
)
@click.option(
    '--mongod', '-m', is_flag=True,
    help='Start local MongoDB database in background process.'
)
def query(uri, config, auto, mongod):

    """ Query a Fast5 collection with PoreMongo """

    if uri == 'local':
        uri = 'mongodb://localhost:27017/poremongo'

    pongo = PoreMongo(
        config=config if config else dict(),
        uri=uri if uri else None
    )

    if pongo.local and mongod:
        pongo.start_mongodb()

    pongo.connect()


    pongo.disconnect()

    if pongo.local and mongod:
        pongo.terminate_mongodb()


