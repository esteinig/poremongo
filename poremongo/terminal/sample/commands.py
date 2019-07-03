import click
import json as js

from pathlib import Path
from poremongo.models import Fast5
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
    '--tags', '-t', type=str, default=None,
    help='Comma separated string for list of tags to query: tag_1,tag_2'
)
@click.option(
    '--proportion', '-p', type=str, default=None,
    help='Proportion to sample across tags, one of: equal, None'
)
@click.option(
    '--json_in', type=Path, default=None,
    help='Process query results (in memory): input query results '
         'from JSON list of Fast5 model objects'
)
@click.option(
    '--json_out', type=Path, default=None,
    help='Process query results (in memory): output query results '
         'as JSON list of objects'
)
@click.option(
    '--unique',  is_flag=True,
    help='Force sampled Fast5 documents to be unique.'
)
@click.option(
    '--display', '-d', is_flag=True,
    help='Print query results in human readable row summary format to STDOUT.'
)
@click.option(
    '--mongod', '-m', is_flag=True,
    help='Start local MongoDB database in background process.'
)
@click.option(
    '--port', '-p', default=27017,
    help='Port for connecting to localhost MongoDB'
)
def sample(
    uri,
    config,
    tags,
    proportion,
    json_in,
    json_out,
    unique,
    display,
    mongod,
    port
):

    """ Query a Fast5 collection with PoreMongo """

    if uri == 'local':
        uri = f'mongodb://localhost:{port}/poremongo'

    pongo = PoreMongo(
        config=config if config else dict(),
        uri=uri if uri else None
    )

    tag_query = tags.split(',')

    prop = proportion.split(',')
    if prop:
        proportion = prop

    if pongo.local and mongod:
        pongo.start_mongodb()

    pongo.connect()

    if json_in:
        with open(json_in, 'r') as infile:
            data = js.load(infile)
            fast5_objects = [Fast5(**entry) for entry in data]
    else:
        fast5_objects = Fast5.objects

    fast5_objects = pongo.sample(
        file_objects=fast5_objects,
        tags=tag_query,
        proportion=proportion,
        unique=unique
    )

    if display:
        print()
        for o in fast5_objects:
            o.to_row()
        print()

    if json_out:
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
        with open(json_out, 'w') as outfile:
            js.dump(data_dict, outfile)

    pongo.disconnect()

    if pongo.local and mongod:
        pongo.terminate_mongodb()


