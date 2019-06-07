"""
Code adapted from Pomoxis: https://github.com/nanoporetech/pomoxis

This Source Code Form is subject to the terms
of the Mozilla Public License, v. 2.0.

(c) 2016 Oxford Nanopore Technologies Ltd.
"""

import warnings

warnings.filterwarnings("ignore")

import os
import json
import logging

from pathlib import Path

from watchdog.observers import Observer
from watchdog.events import RegexMatchingEventHandler
from watchdog.utils import has_attribute, unicode_paths

from paramiko import client

EVENT_TYPE_MOVED = "moved"
EVENT_TYPE_DELETED = "deleted"
EVENT_TYPE_CREATED = "created"
EVENT_TYPE_MODIFIED = "modified"


class SSH:

    def __init__(
        self,
        address="zodiac.hpc.jcu.edu.au",
        port=8822,
        username=None,
        password=None,
        secrets: Path = None,
    ):

        logging.basicConfig(
            level=logging.INFO,
            format="[%(asctime)s] [%(module)-10s] \t %(message)s",
            datefmt="%H:%M:%S",
        )

        if secrets:
            user, password = self._read_secrets(secrets)

        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Connect to server: {address}")

        self.client = client.SSHClient()
        self.client.set_missing_host_key_policy(
            client.AutoAddPolicy()
        )

        self.client.connect(
            address,
            port=port,
            username=username,
            password=password,
            look_for_keys=False,
        )

        self.sftp = self.client.open_sftp()

    @staticmethod
    def _read_secrets(file: Path):

        with file.open("r") as secret:
            data = json.load(secret)
            return data["username"], data["password"]

    def put(self, local_file: str, remote_file: str):

        self.logger.info(
            f"Copy file {local_file} to remote path: {remote_file}"
        )

        self.sftp.put(
            str(local_file), str(remote_file)
        )

        self.logger.info(f"Copied file {local_file} to server.")

    def get(self, remote: Path, local: Path):

        self.logger.info(f"Get file from server: {remote}")

        self.sftp.get(
            str(remote), str(local)
        )

        self.logger.info(f"Copied file from server to: {local}")

    def command(self, command: str):

        self.logger.info(f"Executing command on remote: {command[:20]} ...")
        if self.client:
            stdin, stdout, stderr = self.client.exec_command(command)

            print(stdout)


class FileWatcher:

    """ Watch for new files and transfer to remote UNIX server """

    def __init__(self, ssh: SSH = None):

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s::%(module)s   %(message)s",
            datefmt="%H:%M:%S",
        )

        self.ssh = ssh

        self.logger = logging.getLogger(__name__)

    def watch_path(
        self,
        path: Path,
        callback: callable,
        recursive=False,
        regexes=(".*\.fastq$",),
        **kwargs,
    ):
        """ Watch a filepath for new files, applying callback to files

        :param path: path to watch
        :param callback: callback to apply to newly found files
        :param recursive: recursively watch path?
        :param regexes: filter files by applying regex
        :param kwargs: additional keyword arguments to pass to callback
        """

        self.logger.info("Initiate event handler.")
        handler = StandardRegexMatchingEventHandler(
            callback=callback, regexes=regexes, **kwargs
        )

        self.logger.info("Initiate watchdog event module.")
        watch = Watcher(str(path), event_handler=handler, recursive=recursive)
        self.logger.info(f"Walls to watch: {path}")
        self.logger.info(f"Night gathers, and now my watch begins ...")
        watch.start()

    def callback_transfer(self, fname: str, remote_directory: str):

        if not self.ssh:
            raise ValueError("Transfer callback: could not detect SSH")

        self.ssh.put(
            fname, remote_directory + "/" + fname
        )

    def callback_nextflow(self, fname: str):

        self.logger.info(f"File detected: {Path(fname).name}")


# Helper functions


def wait_for_file(fname):
    """ Block until a file size remains constant """
    size = None
    while True:
        try:
            newsize = os.path.getsize(fname)
        except:
            newsize = None
        else:
            if newsize is not None and size == newsize:
                break

        size = newsize


# EventHandler classes


class StandardRegexMatchingEventHandler(RegexMatchingEventHandler):
    def __init__(self, callback, regexes, **kwargs):
        RegexMatchingEventHandler.__init__(self, regexes=regexes)

        self.callback_arguments = kwargs
        self.callback = callback

    def _process_file(self, event):
        """Process an event when a file is created (or moved).
        :param event: watchdog event.
        :returns: result of applying `callback` to watched file.
        """
        if event.event_type == EVENT_TYPE_CREATED:
            fname = event.src_path
        else:
            fname = event.dest_path
        # need to wait for file to be closed
        wait_for_file(fname)

        return self.callback(fname, **self.callback_arguments)

    def dispatch(self, event):
        """Dispatch an event after filtering. We handle
        creation and move events only.

        :param event: watchdog event.
        :returns: None
        """
        if event.event_type not in (EVENT_TYPE_CREATED, EVENT_TYPE_MOVED):
            return
        if self.ignore_directories and event.is_directory:
            return

        paths = []
        if has_attribute(event, "dest_path"):
            paths.append(unicode_paths.decode(event.dest_path))
        if event.src_path:
            paths.append(unicode_paths.decode(event.src_path))

        if any(r.match(p) for r in self.ignore_regexes for p in paths):
            return

        if any(r.match(p) for r in self.regexes for p in paths):
            self._process_file(event)


class Watcher(object):
    def __init__(self, path, event_handler, recursive=False):
        """Wrapper around common watchdog idiom.
        :param path: path to watch for new files.
        :param event_handler: subclass of watchdog.events.FileSystemEventHandler.
        :param recursive: watch path recursively?
        """
        self.observer = Observer()
        self.observer.schedule(event_handler, path, recursive)

    def start(self):
        """Start observing path."""
        self.observer.start()

    def stop(self):
        """Stop observing path."""
        self.observer.stop()
        self.observer.join()

