import click

from .watch import watch
from .index import index
from .query import query
from .drop import drop

VERSION = '0.3'


@click.group()
@click.version_option(version=VERSION)
def terminal_client():
    """ PoreMongo: Fast5 management in MongoDB """
    pass


terminal_client.add_command(index)
terminal_client.add_command(watch)
terminal_client.add_command(query)
terminal_client.add_command(drop)