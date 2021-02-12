import click
import logging

from pathlib import Path
from functools import partial
from poremongo.poremongo import PoreMongo
from poremongo.poremodels import Read
from poremongo.utils import cli_output

# Monkey patching to show all default options
click.option = partial(click.option, show_default=True)


@click.command()
@click.option(
    '--tags', '-t', type=str, default=None, required=False,
    help='Comma separated string for list of tags to query'
)
@click.option(
    '--db', '-d', type=str, default='poremongo',
    help='Database name to create or connect to'
)
@click.option(
    '--uri', '-u', type=str, default='local',
    help='MongoDB connection: "local" or URI'
)
@click.option(
    '--config', '-c', type=Path, default=None,
    help='Path to config for MongoDB connection [none]'
)
@click.option(
    '--sample', '-s', type=int, default=10,
    help='Number of reads to sample'
)
@click.option(
    '--proportion', '-p', type=str, default=None,
    help='Proportion to sample across tags'
)
@click.option(
    '--display', '-d', is_flag=True,
    help='Print query results in human readable row summary format to STDOUT'
)
@click.option(
    '--quiet', '-q', is_flag=True,
    help='Suppress logging output'
)
@click.option(
    '--unique', is_flag=True,
    help='Force sampled documents to be unique by their ObjectID'
)
@click.option(
    '--json_in', '-ji', type=str, default=None,
    help='Process query results (in memory): input query results from JSON (can be STDIN: -)'
)
@click.option(
    '--json_out', '-jo', type=str, default=None,
    help='Process query results (in memory): output query results as JSON (can be STDOUT: -)'
)
@click.option(
    '--pretty', is_flag=True,
    help='Prettier but reduced display output'
)
def sample(
    uri,
    config,
    tags,
    db,
    proportion,
    json_in,
    json_out,
    unique,
    display,
    quiet,
    sample,
    pretty
):

    """ Sample a Fast5 collection with PoreMongo """

    if uri == 'local':
        uri = f'mongodb://localhost:27017/{db}'

    pongo = PoreMongo(
        config=config if config else dict(),
        uri=uri if uri else None
    )

    if quiet:
        pongo.logger.setLevel(logging.ERROR)

    pongo.connect()

    # Read objects in DB
    read_objects = Read.objects

    tags = [t.strip() for t in tags.split(',')] if tags else []
    proportions = [float(t.strip()) for t in proportion.split(',')] if proportion else []

    read_objects = pongo.sample(
        objects=read_objects, tags=tags, unique=unique, limit=sample, proportion=proportions,
    )

    cli_output(json_out=json_out, read_objects=read_objects, pretty=pretty, display=display)

    pongo.disconnect()



