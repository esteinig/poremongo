import click
import json as js

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
    help='Path to JSON config for MongoDB connection AND raw PyMongo queries.'
)
@click.option(
    '--recursive', '-r', is_flag=True,
    help='Use a recursive path_query to return documents where '
         'path_query is contained in the file path to Fast5'
)
@click.option(
    '--tag', '-t', type=str, default=None,
    help='Comma seperated list of tags to attach to queried results: tag1,tag2'
)
@click.option(
    '--tag_query', type=str, default=None,
    help='Comma separated string for list of tags to query: tag_1,tag_2'
)
@click.option(
    '--name_query', type=str, default=None,
    help='Query for substring in Fast5 file name.'
)
@click.option(
    '--path_query', type=str, default=None,
    help='Exact path query. Use --recursive to execute a partial path_query'
)
@click.option(
    '--shuffle', is_flag=True,
    help='Process query results (in memory): shuffle query objects.'
)
@click.option(
    '--limit', type=int, default=None,
    help='Process query results (in memory): shuffle query objects.'
)
@click.option(
    '--unique', is_flag=True,
    help='Process query results (in memory): set of query objects '
         'to ensure uniqueness.'
)
@click.option(
    '--not_in', is_flag=True,
    help='Reverse a name query by searching for name_query not in name field.'
)
@click.option(
    '--logic', type=str, default='&',
    help='Query logic to chain tag queries.'
)
@click.option(
    '--paths', type=Path, default=None,
    help='Output Fast5 file paths to CSV file.'
)
@click.option(
    '--json', type=Path, default=None,
    help='Process query results (in memory): output query results '
         'as JSON list of objects'
)
@click.option(
    '--display', '-d', is_flag=True,
    help='Print query results in human readable row summary format to STDOUT.'
)
@click.option(
    '--mongod', '-m', is_flag=True,
    help='Start local MongoDB database in background process.'
)
def query(
    uri,
    config,
    tag,
    tag_query,
    name_query,
    path_query,
    recursive,
    not_in,
    logic,
    unique,
    limit,
    shuffle,
    json,
    display,
    paths,
    mongod
):

    """ Query a Fast5 collection with PoreMongo """

    if uri == 'local':
        uri = 'mongodb://localhost:27017/poremongo'

    pongo = PoreMongo(
        config=config if config else dict(),
        uri=uri if uri else None
    )

    tag_query = tag_query.split(',')
    tag = tag.split(',')

    if 'raw_query' in pongo.config.keys():
        raw_query = pongo.config['raw_query']
    else:
        raw_query = None

    if pongo.local and mongod:
        pongo.start_mongodb()

    pongo.connect()

    fast5_objects = pongo.query(
        raw_query=raw_query,
        tag_query=tag_query,
        name_query=name_query,
        path_query=path_query,
        recursive=recursive,
        not_in=not_in,
        query_logic=logic,
    )

    if unique or limit or shuffle:
        fast5_objects = pongo.filter(
            fast5_objects, limit=limit, shuffle=shuffle, unique=unique
        )

    if tag:
        pongo.tag(
            tags=tag,
            raw_query=raw_query,
            tag_query=tag_query,
            name_query=name_query,
            path_query=path_query,
            recursive=recursive,
            not_in=not_in,
        )

    if display:
        print()
        for o in fast5_objects:
            o.to_row()
        print()

    if json:
        if isinstance(fast5_objects, list):
            data_dict = [
                js.loads(
                    o.to_json()
                ) for o in fast5_objects
            ]
        else:
            data_dict = js.loads(
                fast5_objects.to_json()
            )
        with open(json, 'w') as outfile:
            js.dump(data_dict, outfile)

    if paths:
        pongo.paths_to_csv(
            fast5_objects,
            out_file=paths,
        )

    pongo.disconnect()

    if pongo.local and mongod:
        pongo.terminate_mongodb()


