import click

import logging
import json as js

from pathlib import Path
from functools import partial
from poremongo.poremongo import PoreMongo

# Monkey patching to show all default options
click.option = partial(click.option, show_default=True)


@click.command()
@click.option(
    '--uri', '-u', type=str, default='local',
    help='MongoDB URI, "local" or URI'
)
@click.option(
    '--config', '-c', type=Path, default=None,
    help='Path to JSON config for MongoDB connection AND raw PyMongo queries.'
)
@click.option(
    '--recursive', '-r', is_flag=True,
    help='Use a recursive path_query to return documents where '
         'path_query is contained in the file path to Fast5'
)
@click.option(
    '--tags', '-t', type=str, default=None,
    help='Comma separated string for list of tags to query: tag_1,tag_2'
)
@click.option(
    '--fast5', '-f', type=str, default=None,
    help='Exact path query for Fast5 file; use --recursive to execute a query on a part of the path'
)
@click.option(
    '--db', '-d', default='poremongo',
    help='DB to connect to in MongoDB'
)
@click.option(
    '--json', '-j', type=Path, default=None,
    help='Process query results (in memory): output query results as JSON'
)
@click.option(
    '--display', '-d', is_flag=True,
    help='Display query results in human readable format'
)
@click.option(
    '--shuffle', is_flag=True,
    help='Process query results (in memory): shuffle query objects'
)
@click.option(
    '--limit', type=int, default=None,
    help='Process query results (in memory): shuffle query objects'
)
@click.option(
    '--unique', is_flag=True,
    help='Process query results (in memory): set of query objects to ensure uniqueness'
)
@click.option(
    '--not_in', is_flag=True,
    help='Reverse a path query to exclude paths'
)
@click.option(
    '--logic', type=str, default='AND',
    help='Query logic to chain tag queries'
)
@click.option(
    '--add_tags', type=str, default=None,
    help='Comma separated list of tags to attach to queried results and update in DB'
)
@click.option(
    '--update_tags', type=str, default=None,
    help='Comma separated list of `key: tag` and `value: replacement tag` '
         'in format: `key:tag,key:tag` to update tags in queried results'
)
@click.option(
    '--quiet', is_flag=True,
    help='Suppress logging output'
)
def query(
    uri,
    config,
    tags,
    fast5,
    recursive,
    not_in,
    logic,
    unique,
    limit,
    add_tags,
    shuffle,
    json,
    display,
    db,
    update_tags,
    quiet
):

    """ Query a Fast5 collection with PoreMongo """

    if uri == 'local':
        uri = f'mongodb://localhost:27017/{db}'

    pongo = PoreMongo(
        config=config if config else dict(),
        uri=uri if uri else None
    )

    if quiet:
        pongo.logger.setLevel(logging.ERROR)

    if 'raw_query' in pongo.config.keys():
        raw_query = pongo.config['raw_query']
    else:
        raw_query = None

    pongo.connect()

    read_objects = pongo.query(
        raw_query=raw_query,
        tag_query=[t.strip() for t in tags.split(',')] if tags else None,
        path_query=fast5,
        recursive=recursive,
        not_in=not_in,
        query_logic=logic
    )

    if unique or limit or shuffle:
        read_objects = pongo.filter(
            read_objects, limit=limit, shuffle=shuffle, unique=unique
        )

    if add_tags:
        pongo.tag(
            tags=[t.strip() for t in add_tags.split(',')],
            raw_query=raw_query,
            tag_query=tags,
            path_query=fast5,
            recursive=recursive,
            not_in=not_in
        )

    if display:
        for o in read_objects:
            print(o)

    if json:
        if isinstance(read_objects, list):
            data_dict = [js.loads(o.to_json()) for o in read_objects]
        else:
            data_dict = js.loads(
                read_objects.to_json()
            )

        if json == "-":
            for o in data_dict:
                print(o)
        else:
            with open(json, 'w') as outfile:
                js.dump(data_dict, outfile)

    pongo.disconnect()


