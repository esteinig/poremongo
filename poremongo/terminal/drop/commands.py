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
    '--auto', '-a', is_flag=True,
    help='Automatically confirm drop without confirmation prompt.'
)
@click.option(
    '--mongod', '-m', is_flag=True,
    help='Start local MongoDB database in background process.'
)
@click.option(
    '--port', '-p', default=27017,
    help='Port for connecting to localhost MongoDB'
)
def drop(uri, config, auto, mongod, port):

    """ Drop the database at the given URI """

    if uri == 'local':
        uri = f'mongodb://localhost:{port}/poremongo'

    db = Path(uri).stem

    pongo = PoreMongo(
        config=config if config else dict(),
        uri=uri if uri else None
    )

    if not auto:
        try:
            click.confirm(
                f'Drop database: {db} at {pongo.decompose_uri()}'
            )
        except click.Abort:
            print('Drop terminated. Exiting.')
            exit(0)

    if pongo.local and mongod:
        pongo.start_mongodb()

    pongo.connect()
    pongo.client.drop_database(db)

    pongo.logger.info(
        f'Dropped database at {pongo.decompose_uri()}'
    )

    pongo.disconnect()

    if pongo.local and mongod:
        pongo.terminate_mongodb()


