#!/usr/bin/env python

import os
import sys
from http.client import responses
from typing import Annotated, Callable, NoReturn, Optional

import typer
from dateutil import parser
from rich.console import Console, Group
from rich.live import Live
from rich.pretty import pprint
from rich.progress import (BarColumn, MofNCompleteColumn, Progress,
                           SpinnerColumn, TaskProgressColumn, TextColumn,
                           TimeRemainingColumn)
from rich.table import Column, Table

from .client import SyncClient
from .typedefs import Config, DetailedClientResponseError, SPFile, TrackingStep
from .utils import human_readable_size, load_config, or_else, panic

__all__ = ['main']


# ------------------------------------------------------------------------------------------- Consts

DEFAULT_CONFIG_PATH = './config.toml'

app = typer.Typer(
    name='spdl',
    help='Resolve and download SharePoint site/drive files located on a distant server',
    no_args_is_help=True
)
console = Console()
unknown_progress = Progress(
    SpinnerColumn(),
    TextColumn("[progress.description]{task.description}"),
    transient=True,
    console=console
)
known_progress = Progress(TextColumn(
    "[progress.description]{task.description}"),
    BarColumn(),
    TaskProgressColumn(),
    TimeRemainingColumn(),
    MofNCompleteColumn(),
    console=console
)
group = Group(unknown_progress, known_progress)
live = Live(group, console=console, refresh_per_second=10)


# -------------------------------------------------------------------------------------------- Utils

tasks = {}
def on_advance(action_name: str, step: TrackingStep, count: int) -> None:
    if step == 'start':
        if action_name in tasks:
            raise Exception(f'Task {action_name} is already subscribed')
        progress = unknown_progress if count is None else known_progress
        tasks[action_name] = (progress, progress.add_task(action_name, total=count))
        return
    
    progress, task_id = tasks[action_name]
    if step == 'progress':
        count = 1 if count is None else count
        progress.advance(task_id, count)
        return
    
    # if step == 'end'
    progress.remove_task(task_id)
    tasks.pop(action_name, None)


def parse_arguments(
    config          : str,
    queue_size      : Optional[int] = None,
    worker_count    : Optional[int] = None,
    page_size       : Optional[int] = None,
    logging_level   : Optional[int] = None,
    logging_file    : Optional[str] = None
) -> Config:    
    config = load_config(config)

    if queue_size is not None:
        config['client']['queue_size'] = queue_size
    if worker_count is not None:
        config['client']['worker_count'] = worker_count
    if page_size is not None:
        config['client']['page_size'] = page_size
    if logging_level is not None:
        config['client']['logging_level'] = logging_level
    if logging_file is not None:
        config['client']['logging_files'] = [logging_file]
    
    return config


def nice_errors[T, U](debug: bool) -> Callable[[Callable[[T], U]], Callable[[T], U | NoReturn]]:
    def decorator(f: Callable[[T], U]): 
        def wrapped_f(*args, **kwargs):
            if debug:
                return f(*args, **kwargs)
            try:
                return f(*args, **kwargs)
            except DetailedClientResponseError as cre:
                console.print(f'{cre.request_info.method} {cre.request_info.url}')
                console.print(f'HTTP/1.1 [bold red]{cre.status}[/] {responses[cre.status]}')
                console.print(cre.body)
                sys.exit(1)
        return wrapped_f
    return decorator

# ------------------------------------------------------------------------ CLI arguments annotations

URLAnnotation = Annotated[str, typer.Argument(
    help='The sharepoint resource location (URL)',
    show_default=False
)]

PageAnnotation = Annotated[Optional[int], typer.Option(
    '--page-size',
    '-p',
    help='The maximum number of elements available per request',
    show_default=False
)]

ConfigAnnotation = Annotated[str, typer.Option(
    '--config',
    '-c',
    help='Configuration file location'
)]

VerbosityAnnotation = Annotated[int, typer.Option(
    '--verbose',
    '-v',
    count=True,
    help='Logging verbosity, specifying this option multiple times further increases verbosity',
    show_default=False
)]

LoggingFileAnnotation = Annotated[str, typer.Option(
    '--logging-file',
    '-l',
    help='The file in which logging messages shall be sent, defaults to stderr',
    show_default=False
)]

PageSizeAnnotation = Annotated[Optional[int], typer.Option(
    '--page-size',
    '-p',
    help='The maximum number of elements available per request',
    show_default=False
)]

DirectoryAnnotation = Annotated[Optional[str], typer.Option(
    '--dir',
    '-d',
    help='The directory wherein downloaded files and folders should be placed',
    show_default=False
)]

WorkerCountAnnotation = Annotated[Optional[int], typer.Option(
    '--workers',
    '-w',
    help='The number of workers to use for simultaneous files downloads',
    show_default=False
)]

QueueSizeAnnotation = Annotated[Optional[int], typer.Option(
    '--queue-size',
    '-q',
    help='The maximum queue size, a negative number unbounds the queue',
    show_default=False
)]


# ------------------------------------------------------------------------------------------ Resolve

@app.command(name='resolve')
def resolve_command(
    url         : URLAnnotation,
    config      : ConfigAnnotation      = DEFAULT_CONFIG_PATH,
    verbose     : VerbosityAnnotation   = 0,
    logging_file: LoggingFileAnnotation = None
) -> None:
    '''Resolve the graph path of the given URL from the root site id to the final directory/file,
    resolving any intermediate site and/or drive if necessary'''
    config = parse_arguments(config, logging_level=verbose, logging_file=logging_file)
    with SyncClient(config, on_advance, console) as sharepoint:
        resolution = sharepoint.resolution(url)

    final_site_id = None
    final_drive_id = None
    final_file = None

    table = Table('Endpoint', 'Site', 'Drive', 'File')
    for (endpoint_segment, site_id, drive_id, spfile) in resolution:
        if site_id is not None:
            final_site_id = site_id
        site_id = or_else(site_id, '')
        site_id = site_id.split(',')[-1]

        if drive_id:
            final_drive_id = drive_id
        drive_id = or_else(drive_id, "N/A")

        if spfile:
            final_file = spfile
        file_id = spfile['id'] if spfile else 'N/A'
        table.add_row(endpoint_segment, site_id, drive_id, file_id)

    console.print(table)
    console.print(f'Site: {final_site_id}')
    if final_drive_id:
        console.print(f'Drive: {final_drive_id}')
    if final_file:
        file_type = "Folder" if SPFile.is_folder(final_file) else "File"
        console.print(f'{file_type}: {final_file["id"]}')


# --------------------------------------------------------------------------------------------- List

@app.command(name='list')
def list_command(
    url         : URLAnnotation,
    config      : ConfigAnnotation      = DEFAULT_CONFIG_PATH,
    verbose     : VerbosityAnnotation   = 0,
    logging_file: LoggingFileAnnotation = None,
    page_size   : PageSizeAnnotation    = None
) -> None:
    '''List the files and directories available at the given URL'''
    config = parse_arguments(config, None, None, page_size, verbose, logging_file)

    total_size = 0
    file_number = 0
    folder_number = 0
    spfiles = []
    with SyncClient(config, on_advance, console) as sharepoint:
        resolution = sharepoint.resolve(url)
        site_id, drive_id, spfile = resolution

        if spfile is not None and SPFile.is_file(spfile):
            spfiles = [spfile]
        
        elif drive_id is None:
            panic('Not a drive or folder')
        
        else:
            folder_id = spfile['id'] if spfile else None
            spfiles, _ = sharepoint.list((site_id, drive_id, folder_id), True, True)
        
    table = Table(
        "Name",
        Column("Size", justify='right'),
        Column("Children", justify='right'),
        Column("Created", justify='right'),
        Column("Last Updated", justify='right'),
    )
    for spfile in spfiles:

        name = spfile['name']
        file_size = spfile['size']
        children = str(spfile.get('folder', {}).get('childCount', 'N/A'))
        creation_date = parser.parse(spfile['createdDateTime'])
        update_date = parser.parse(spfile['lastModifiedDateTime'])
        table.add_row(
            name,
            human_readable_size(file_size),
            children,
            creation_date.strftime("%d/%m/%Y %H:%M:%S"),
            update_date.strftime("%d/%m/%Y %H:%M:%S")
        )

        total_size = total_size + file_size
        file_number = file_number + (1 if SPFile.is_file(spfile) else 0)
        folder_number = folder_number + (1 if SPFile.is_folder(spfile) else 0)

    console.print(table)
    size = human_readable_size(total_size)
    console.print(f'{file_number} file(s), {folder_number} folder(s), {size} total')


# ----------------------------------------------------------------------------------------- Download

@app.command(name='download')
def download_command(
    url         : URLAnnotation,
    directory   : DirectoryAnnotation   = None,
    config      : ConfigAnnotation      = DEFAULT_CONFIG_PATH,
    verbose     : VerbosityAnnotation   = 0,
    logging_file: LoggingFileAnnotation = None,
    worker_count: WorkerCountAnnotation = None,
    queue_size  : QueueSizeAnnotation   = None,
    page_size   : PageAnnotation        = None,
) -> None:
    '''Download the file/directory located at the given URL'''
    config = parse_arguments(config, queue_size, worker_count, page_size, verbose, logging_file)
    with SyncClient(config, on_advance) as sharepoint:
        sharepoint.download(url, directory)


# ------------------------------------------------------------------------------------------ Request

@app.command(name='request')
def request_command(
    url         : URLAnnotation,
    config      : ConfigAnnotation      = DEFAULT_CONFIG_PATH,
    verbose     : VerbosityAnnotation   = 0,
    logging_file: LoggingFileAnnotation = None
) -> None:
    '''Execute the given request with the configured authentication'''
    config = parse_arguments(config, logging_level=verbose, logging_file=logging_file)
    with SyncClient(config, on_advance) as sharepoint:
        result = sharepoint.request(url)
        pprint(result, console=console)


# --------------------------------------------------------------------------------------------- Main

@nice_errors(os.environ.get('TRACEBACK') is not None)
def main() -> int:
    with live:
        app()


if __name__ == '__main__':
    main()
