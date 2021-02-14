import click
import logging

from pathlib import Path
from functools import partial
from poremongo.poremongo import PoreMongo

# Monkey patching to show all default options
click.option = partial(click.option, show_default=True)


@click.command()
@click.option(
    '--uri', '-u', type=str, default='local',
    help='MongoDB connection: "local" or URI'
)
@click.option(
    '--db', '-d', type=str, default="poremongo",
    help='Name of database to connect to [poremongo]'
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
    '--total', '-t', is_flag=True,
    help='Add a total count of reads in --db [false]'
)
@click.option(
    '--quiet', is_flag=True,
    help='Suppress logging output'
)
def check(uri, config, db, quiet):

    """ Create a sampled dataset """

    if uri == 'local':
        uri = f'mongodb://localhost:27017/{db}'

    pongo = PoreMongo(
        config=config if config else dict(),
        uri=uri if uri else None
    )

    if quiet:
        pongo.logger.setLevel(logging.ERROR)

    pongo.logger.info("Conducting database connection check")

    pongo.connect()
    pongo.disconnect()


