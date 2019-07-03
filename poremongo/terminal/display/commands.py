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
    help='Path to JSON config file for MongoDB connection.'
)
@click.option(
    '--limit', '-l', type=int, default=100,
    help='Limit the number of unique tags on display.'
)
@click.option(
    '--mongod', '-m', is_flag=True,
    help='Start local MongoDB database in background process.'
)
@click.option(
    '--port', '-p', default=27017,
    help='Port for connecting to localhost MongoDB'
)
def display(uri, config, limit, mongod, port):

    """ Display most common tags in the database """

    if uri == 'local':
        uri = f'mongodb://localhost:{port}/poremongo'

    pongo = PoreMongo(
        config=config if config else dict(),
        uri=uri if uri else None
    )



    if pongo.local and mongod:
        pongo.start_mongodb()

    pongo.connect()

    counts, _ = pongo.get_tag_data(limit=limit)

    for count in counts:
        print(count)


    pongo.disconnect()

    if pongo.local and mongod:
        pongo.terminate_mongodb()


