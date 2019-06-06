import click

from .index import index


VERSION = '0.3'


@click.group()
@click.version_option(version=VERSION)
def terminal_client():
    """ PoreMongo: Fast5 management in MongoDB """
    pass


terminal_client.add_command(index)
