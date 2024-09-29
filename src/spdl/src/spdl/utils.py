#!/usr/bin/env python


import json
import logging
import sys
from collections import deque
from datetime import datetime, timedelta
from functools import reduce
from logging import FileHandler, Logger, StreamHandler, getLogger
from pathlib import PurePosixPath
from typing import (Callable, Deque, List, NoReturn, Optional, TextIO, Tuple,
                    TypeVar)
from urllib.parse import unquote, urlparse

import aiofiles
import aiofiles.os
import msal
import toml
from dateutil import parser
from rich.console import Console
from rich.logging import RichHandler

from .typedefs import (AuthToken, Config, ConfigException, Identifier, SPFile,
                     SPResponse)

# Consts

LOGGER = logging.getLogger("spdl")
err_console = Console(file=sys.stderr)

# Types

T = TypeVar('T')


# Utils

def trace[T](t:T) ->T:
    print(t, file=sys.stderr)
    return t

class AuthenticationException(Exception):
    pass

def login(config: Config) -> AuthToken:
    '''
    Logs in using the provided configuration, on success the configuration is
    returned with the token available in the 'token' key.
    '''
    server_config = config['server']
    auth_config = config['auth']

    with open(auth_config['private_key_path'], 'r') as pkf:
        private_key = pkf.read()
    
    app = msal.ConfidentialClientApplication(
        auth_config['client_id'],
        authority=f'https://login.microsoftonline.com/{server_config["tenant_id"]}',
        client_credential={
            'thumbprint': auth_config['cert_thumbprint'],
            'private_key': private_key
        }
    )

    auth_token = app.acquire_token_silent(auth_config['scopes'], account=None)

    if not auth_token:
        LOGGER.debug('No suitable token exists in cache. Retreiving one from AAD.')
        auth_token = app.acquire_token_for_client(auth_config['scopes'])

    if 'access_token' not in auth_token:
        LOGGER.error('Unable to login to AAD')
        raise AuthenticationException(**auth_token)

    auth_token['expires_at'] = datetime.now() + timedelta(0, auth_token['expires_in'])
    
    return auth_token


def expired(auth_token: Optional[AuthToken], safety_delta: int = 300) -> bool:
    if auth_token is None:
        return True
    expiration_date = (auth_token or {}).get('expires_at')
    now = datetime.now() - timedelta(0, safety_delta)
    return expiration_date is None or expiration_date < now


def find_named(data: Optional[SPResponse], name: str) -> Optional[SPFile]:
    if data is None or 'value' not in data:
        return None
    target = [ x for x in data['value'] if x['name'] == name ]
    return target[0] if target else None


async def mkdir(directory_path: str) -> None:
    if await aiofiles.os.path.exists(directory_path):
        if not await aiofiles.os.path.isdir(directory_path):
            LOGGER.error(f'Error: {directory_path} is not a directory')
            sys.exit(1) # FIXME EXCEPTION INSTEAD
    else:
        await aiofiles.os.makedirs(directory_path, exist_ok=True)


def to_snake_case(object: any, excluded_keys: List[str]) -> any:
    if not isinstance(object, dict):
        return object
    updated = {}
    for k, v in object.items():
        if k in excluded_keys:
            updated[k] = v
            continue 
        if isinstance(k, str):
            updated[k.replace('-', '_')] = to_snake_case(v, excluded_keys)
        else:
            updated[k] = to_snake_case(v, excluded_keys)
    return updated


def load_config(file_path: str) -> Config:
    with open(file_path, "r") as file_content:
        return to_snake_case(toml.load(file_content), ['sites'])


def resolve_site_id(config: Config, url: str) -> Tuple[str, Optional[Identifier]]:
    registered_hosts = config.get('sites')
    if registered_hosts is None:
        raise ConfigException('sites', None)

    target = url.replace('http://', '').replace('https://', '')
    for host_url, host_id in registered_hosts.items():
        host = host_url.replace('http://', '').replace('https://', '')
        if host in target:
            return target.replace(host, ''), host_id

    return url, None


def configure_logging(verbosity: int, logging_file: Optional[str] = None) -> None:
    logging_level = logging.ERROR - (verbosity * 10)
    for logger_name in ['httpcore', 'hpack', 'httpx', 'msal', 'urllib3']:
        logging.getLogger(logger_name).setLevel(logging.ERROR)
    logging.basicConfig(level=max(10, logging_level), filename=logging_file)
    LOGGER.setLevel(max(10, logging_level))


def or_else(value: Optional[T], default: T) -> T:
    return default if value is None else value


def panic(message: str) -> NoReturn:
    err_console.print(f'[bold red]ERROR: {message}[/bold red]')
    sys.exit(1)


# in place cons, deque for complexity (O(1) instead of O(N), both in time and space)
def append[T](xs: Deque[T], x: T) -> Deque[T]:
    xs.append(x)
    return xs


def partition[T](p: Callable[[T], bool], xs: List[T]) -> Tuple[List[T], List[T]]:
    left, right = reduce(
        lambda a, x: (append(a[0], x), a[1]) if p(x) else (a[0], append(a[1], x)),
        xs,
        (deque(), deque())
    )
    return list(left), list(right)


def content_url(spfile: SPFile) -> Optional[str]:
    return spfile.get('@microsoft.graph.downloadUrl')


async def is_up_to_date(file_path: str, new_spfile: SPFile) -> bool:

    file_exists = await aiofiles.os.path.exists(file_path)
    metadata_exists = await aiofiles.os.path.exists(f'{file_path}.metadata')
    if not file_exists or not metadata_exists:
        return False

    try:
        
        async with aiofiles.open(f'{file_path}.metadata', 'r') as fin:
            content = await fin.read()
            last_spfile = json.loads(content)
            last_download = parser.parse(last_spfile['lastModifiedDateTime'])
            new_download = parser.parse(new_spfile['lastModifiedDateTime'])
            return last_download >= new_download
        
    except Exception as e:
        LOGGER.warning(e)
        return False


def human_readable_size(size: int) -> str:
    if size > (1024 * 1024 * 1024):
        return '{0:.2f} GB'.format(size / (1024 * 1024 * 1024))
    if size > (1024 * 1024):
        return '{0:.2f} MB'.format(size / (1024 * 1024))
    if size > 1024:
        return '{0:.2f} KB'.format(size / 1024)
    return f'{size} B'


def create_logger(
    name: str,
    logger: Optional[Logger] = None,
    logging_level: int = 2,
    console: Optional[Console] = None,
    files: Optional[List[str | TextIO]] = None
) -> Logger:
    
    logger_level = max(0, 50 - (logging_level * 10))
    if logger is not None:
        logger.setLevel(logger_level)
        return logger
    
    formatter = logging.Formatter(
        '{asctime} - {levelname} - {message}',
        style='{',
        datefmt='%d-%m-%Y %H:%M',
    )

    handlers = []
    if console is not None:
        handlers.append(RichHandler(level=logger_level, console=console))
    
    files = [] if files is None else files
    for file in files:
        handler = FileHandler(file) if isinstance(file, str) else StreamHandler(file)
        handler.setFormatter(formatter)
        handlers.append(handler)
    
    logger = getLogger(name)
    for handler in handlers:
        logger.addHandler(handler)
    
    logger.setLevel(logger_level)
    return logger


def resolve_local_dir(url: str, target_dir: Optional[str] = None) -> str:
    if target_dir is not None:
        return target_dir
    url_path = PurePosixPath(unquote(urlparse(url).path)).parts
    targets_file = len(url_path[-1].split('.')) > 1
    if targets_file:
        if len(url_path) <= 1:
            return './'
        return f'./{url_path[-2]}'
    return f'./{url_path[-1]}'
