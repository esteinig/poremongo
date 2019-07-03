import time
import click
import time

from pathlib import Path
from functools import partial

from poremongo.watchdog import FileWatcher
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
    '--path', '-p', type=Path, required=True,
    help='Path to watch for incoming Fast5 files.'
)
@click.option(
    '--recursive', '-r', is_flag=True,
    help='Recursively watch path for Fast5 files.'
)
@click.option(
    '--mongod', '-m', is_flag=True,
    help='Start local MongoDB database in background process.'
)
@click.option(
    '--port', '-p', default=27017,
    help='Port for connecting to localhost MongoDB'
)
def watch(uri, config, path, recursive, mongod, port):
    """ Watch a directory for incoming Fast5 and upsert to DB """

    if uri == 'local':
        uri = f'mongodb://localhost:{port}/poremongo'

    pongo = PoreMongo(
        config=config if config else dict(),
        uri=uri if uri else None
    )

    if pongo.local and mongod:
        pongo.start_mongodb()

    pongo.connect()

    fw = FileWatcher()
    fw.logger.info('File watcher initiated, start observation')

    fw.watch_path(
        path=path,
        callback=pongo.callback_insert,
        recursive=recursive,
        regexes=(".*\.fast5",)
    )

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        fw.stop_watch()
        fw.logger.info('File watcher interrupted by user. Exiting.')
        exit(0)
    except SystemError:
        fw.stop_watch()
        fw.logger.info('File watcher interrupted by system. Exiting.')
        exit(0)
    finally:
        pongo.disconnect()
        if pongo.local and mongod:
            pongo.terminate_mongodb()

