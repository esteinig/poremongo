import click
import logging

from pathlib import Path
from functools import partial
from poremongo.poremongo import PoreMongo
from poremongo.poremodels import Read
# Monkey patching to show all default options
click.option = partial(click.option, show_default=True)

from colorama import Fore

Y = Fore.YELLOW
G = Fore.GREEN
RE = Fore.RESET

@click.command()
@click.option(
    '--uri', '-u', type=str, default='local',
    help='MongoDB connection: "local" or URI'
)
@click.option(
    '--db', '-d', type=str, default="poremongo",
    help='Name of database to connect to'
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
    help='Add a total count of reads in --db'
)
@click.option(
    '--quiet', is_flag=True,
    help='Suppress logging output'
)
def counts(uri, config, limit, db, quiet, total):

    """ Display most common tags in the database """

    if uri == 'local':
        uri = f'mongodb://localhost:27017/{db}'

    pongo = PoreMongo(
        config=config if config else dict(),
        uri=uri if uri else None
    )

    if quiet:
        pongo.logger.setLevel(logging.ERROR)

    pongo.connect()

    _counts = pongo.get_tag_counts(limit=limit)

    print()
    print(f"{Y}{'tag':46}\t{G}{'read_count':<18}{RE}")
    print(64*"-")
    for tag in _counts:
        print(f'{Y}{tag["_id"]:<46}\t{G}{tag["count"]:<18}{RE}')

    if total:
        print(f'\n{Y}{"total":<46}\t{G}{len(list(Read.objects)):<18}{RE}')
    print(64 * "-")
    print()

    pongo.disconnect()


