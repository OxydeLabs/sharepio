#!/usr/bin/env python


import asyncio
import json
import logging
import sys
from asyncio import Queue
from functools import cached_property
from logging import Logger
from typing import Dict, List, Optional, Self, Tuple, TypeVar
from urllib.parse import ParseResult, urlparse

import aiofiles
import aiofiles.os
import aiohttp
import validators
from aiohttp import (ClientResponse, ClientResponseError, ClientSession,
                     ClientTimeout)
from async_lru import alru_cache
from rich.console import Console

from ..typedefs import (AuthToken, Config, DetailedClientResponseError,
                      Identifier, Resolution, ResolutionException,
                      SegmentException, SPFile, SPResponse, ThrottleException,
                      Tracker, TrackingStep)
from ..utils import (create_logger, expired, find_named, human_readable_size,
                     is_up_to_date, login, mkdir, partition, resolve_local_dir,
                     resolve_site_id)
from .client import AnyGraphResponse, BatchGraphResponse, Client, GraphResponse

# Consts

DEFAULT_CONFIG_PATH = './config.toml'

GRAPH_API_HOST: str = 'graph.microsoft.com' 
GRAPH_API_URL: str = f'https://{GRAPH_API_HOST}/v1.0' 
GRAPH_API_BATCH_URL: str = f'{GRAPH_API_URL}/$batch'

MAX_GRAPH_BATCH_SIZE = 20
DEFAULT_PAGE_SIZE = 500
DEFAULT_QUEUE_SIZE = 256


# Types

T = TypeVar('T')
U = TypeVar('U')

__all__ = ['AsyncClient']


class AsyncClient(Client):

    session: ClientSession
    '''The session used to perform REST requests.'''
    config: Config
    '''Knobs, latches and levers to configure the client behaviours.'''
    token: AuthToken
    '''Authentication token used against the graph API authentication mechanism.'''
    tracker: Optional[Tracker]
    '''An optional function to be called when operation(s) progress occur.'''
    console: Console
    '''The console used to communicate messages to the user'''
    logger: Logger
    '''TODO'''

    # Ctor

    def __init__(
        self,
        config: Config,
        tracker: Optional[Tracker] = None,
        console: Optional[Console] = None
    ):
        '''Instanciates a SharePoint client.
        
        :param Config config: The client configuration, site identifiers, proxies, worker counts,
        etc.
        :param Tracker tracker: An optional progress tracker to be called when operation(s)
        progress occur
        :param Console console: The rich console to use for printing, if any
        '''
        self.session = None
        self.config = config
        self.token = None
        self.tracker = tracker
        self.console = Console(file=sys.stderr) if console is None else console
        logging_files = config.get('client', {}).get('logging_files', [sys.stderr])
        self.logger = create_logger(
            AsyncClient.__name__,
            None,
            config.get('client', {}).get('logging_level', 1),
            console,
            logging_files
        )

    # Ctx

    def start(self) -> None:
        timeout = ClientTimeout(self.timeout_window)
        self.session = aiohttp.ClientSession(timeout=timeout)
    

    async def close(self) -> None:
        await self.session.close()
        self.session = None


    async def __aenter__(self) -> Self:
        self.start()
        return self


    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


    # Props

    @property
    def authorization(self) -> str:
        '''The bearer token to be used as *Authentication* header within HTTP requests.'''
        if expired(self.token):
            self.token = login(self.config)
        token_type, access_token = self.token['token_type'], self.token['access_token']
        return f'{token_type} {access_token}'


    @cached_property
    def worker_count(self) -> int:
        '''The number of asynchronous workers. A worker is a coroutine whose purpose is to fetch
        and write files (i.e. a consumer).'''
        return self.config.get('client', {}).get('worker_count', 8)


    @cached_property
    def batch_size(self) -> int:
        return self.config.get('client', {}).get('batch_size', MAX_GRAPH_BATCH_SIZE)


    @cached_property
    def timeout_window(self) -> int:
        '''Maximum duration a request, and its consumption, may take. Any request taking longer than
        the specified duration (in seconds) will raise a *TimeoutException*.'''
        return self.config.get('client', {}).get('timeout_window', 300)


    @cached_property
    def proxy(self) -> Optional[str]:
        '''A proxy through which to redirect requests, if any.'''
        return self.config.get('client', {}).get('proxy')


    @cached_property
    def page_size(self) -> int:
        return self.config.get('client', {}).get('page_size', DEFAULT_PAGE_SIZE)


    @cached_property
    def queue_size(self) -> int:
        return self.config.get('client', {}).get('queue_size', DEFAULT_QUEUE_SIZE)

    # Logging

    def _log(self, level: int, message: str, *args, **kwargs) -> None:
        self.logger.log(level, message, *args, **kwargs)

    # Progress tracking

    def _track(
        self,
        action_name: str,
        step: TrackingStep,
        count: Optional[int] = None,
        tracked: bool = True
    ) -> None:
        '''Informs the tracker that the action ``action_name`` has performed a ``step``'''
        if not tracked or not self.tracker:
            return
        self.tracker(action_name, step, count)


    # Request

    def _filtered(self, url: str, filters: Dict[str, str]) -> str:
        
        if (len(filters)) == 0:
            return url
        
        url: ParseResult = urlparse(url)
        query_parts = filter(lambda x: x != '', url.query.split('&'))

        filter_query = None
        queries = []
        for query_part in query_parts:
            k, v = query_part.split('=')
            if k == '$filter':
                filter_query = v
            else:
                queries.append(query_part)
        
        given_filter = map(lambda kv: f'{kv[0]} eq {kv[1]}', filters.items())
        given_filter = ','.join(given_filter)
        if filter_query is None:
            filter_query = f'and({given_filter})'
        else:
            filter_query = f'and({filter_query}, {given_filter})'

        queries.append(filter_query)
        url._replace(query='&'.join(queries))
        return url.geturl()


    def _url(self, endpoint: str | Resolution) -> str:
        if isinstance(endpoint, str):
            if validators.url(endpoint):
                return endpoint    
            return f'{GRAPH_API_URL}{endpoint}'
        # else
        site_id, drive_id, file_id = endpoint
        if isinstance(file_id, dict):
            file_id = file_id['id']
        
        if file_id is None:
            return self._url(f'/sites/{site_id}/drives/{drive_id}/root')
        return self._url(f'/drives/{drive_id}/items/{file_id}')


    async def _handle_response(self, response: ClientResponse) -> SPResponse | bytes | None:
        is_json = 'application/json' in (response.headers.get('Content-Type') or '')
        # FIXME this forces the loading of the whole file in memory
        body = await (response.json() if is_json else response.content.read())

        if response.status == 429:
            raise ThrottleException(body.get('error', {}).get('retryAfterSeconds', 10))
        try:
            response.raise_for_status()
        except ClientResponseError as cre:
            raise DetailedClientResponseError.wrap(cre, body) from cre

        if response.status == 204:
            return None
        
        return body


    async def _request_single[T : GraphResponse](
        self,
        endpoint: str,
        headers: Optional[Dict[str, str]] = None,
        filters: Optional[Dict[str, str]] = None
    ) -> Optional[T]:
        try:
            endpoint = self._url(endpoint)
            headers = {} if headers is None else headers
            filters = {} if filters is None else filters
            endpoint = self._filtered(endpoint, filters)
            headers['Authorization'] = self.authorization
            self._log(logging.DEBUG, f'GET request on \'{endpoint}\'')
            async with self.session.get(endpoint, headers=headers, proxy=self.proxy) as response:
                return await self._handle_response(response)
        except ThrottleException as te:
            self._log(logging.WARN, '[bold orange]Throttled![/]', extra={ 'markup': True })
            await asyncio.sleep(te.throttle_time)
            return await self._request_single(endpoint)


    async def _request_batch[T : BatchGraphResponse](
        self,
        endpoints: List[str],
        headers: Optional[Dict[str, str]]
    ) -> T:

        # Unused. As it is we can't batch request files downloads which this method was primarily
        # meant to do. Other uses cases exist, we could optimize other calls, so I leave this corpse
        # here for now.

        headers = {} if headers is None else headers
        headers['Authorization'] = self.authorization
        requests = [{
            'id': i,
            'method': 'GET',
            'url': self._url(endpoint)
        } for i, endpoint in enumerate(endpoints)]
        request_payload = { 'requests': requests }
        
        async with self.session.post(
            GRAPH_API_BATCH_URL,
            headers=headers,
            json=request_payload
        ) as response:
            responses = await self._handle_response(response)
            if responses is None:
                return None
            responses = sorted(responses.get('responses', []), key=lambda r: r['id'])
            succeeded = []
            failed = []
            for endpoint, response in zip(endpoints, responses):
                if response['status'] != 200:
                    failed.append((endpoint, response))
                succeeded.append(response['body']['value'])

            return succeeded, failed

    # Finders

    async def _find_subsite(self, site_id: Identifier, site_name: str) -> Optional[SPFile]:
        self._log(logging.DEBUG, f'Attempting to find subsite \'{site_name}\' at site \'{site_id}\'')
        return find_named(
            await self._request_single(f'/sites/{site_id}/sites?$filter=name eq \'{site_name}\''),
            site_name
        )
    

    async def _find_drive(self, site_id: Identifier, drive_name: str) -> Optional[SPFile]:
        self._log(logging.DEBUG, f'Attempting to find drive \'{drive_name}\' at site \'{site_id}\'')
        return find_named(
            await self._request_single(
                f'/sites/{site_id}/drives',
                filters={ 'name': drive_name }
            ),
            drive_name
        )


    async def _find_drive_object(
        self,
        site_id: Identifier,
        drive_id: Identifier,
        object_name: str
    ) -> Optional[SPFile]:
        self._log(logging.DEBUG, f'Attempting to find drive object \'{object_name}\' in site \'{site_id}\'/drive \'{drive_id}\'')
        return find_named(
            await self._request_single(
                f'/sites/{site_id}/drives/{drive_id}/root/children',
                filters={ 'name': object_name }
            ),
            object_name
        )


    async def _find_drive_folder(
        self,
        site_id: Identifier,
        drive_id: Identifier,
        folder_name: str
    ) -> Optional[SPFile]:
        target = await self.find_drive_object(site_id, drive_id, folder_name)
        return target if SPFile.is_folder(target) else None


    async def _find_folder_object(
        self,
        drive_id: Identifier,
        folder_id: Identifier,
        object_name: str
    ) -> Optional[SPFile]:
        self._log(logging.DEBUG, f'Attempting to find folder object \'{object_name}\' in drive \'{drive_id}\'/folder \'{folder_id}\'')
        return find_named(
            await self._request_single(
                f'/drives/{drive_id}/items/{folder_id}/children',
                filters={ 'name': object_name }
            ),
            object_name
        )


    async def _find_folder_folder(
        self,
        drive_id: Identifier,
        folder_id: Identifier,
        folder_name: str
    ) -> Optional[SPFile]:
        target = await self._find_folder_object(drive_id, folder_id, folder_name)
        return target if SPFile.is_folder(target) else None

    # Resolution

    def _determine_site_id(
        self,
        endpoint: str,
        site_id: Optional[Identifier] = None
    ) -> Tuple[str, Optional[Identifier]]:
        if site_id is not None:
            return endpoint, site_id
        return resolve_site_id(self.config, endpoint)


    async def _resolve_rec(
        self,
        site_id: Identifier,
        endpoint: str,
        drive_id: Optional[Identifier] = None,
        spfile: Optional[SPFile] = None,
        resolution: List[Tuple[str, Optional[Identifier], Optional[Identifier], Optional[SPFile]]] = None
    ) -> List[Tuple[str, Optional[Identifier], Optional[Identifier], Optional[SPFile]]]:
        
        if spfile is not None and SPFile.is_file(spfile) and len(endpoint) > 0:
            raise SegmentException() # TODO
        
        if len(endpoint) == 0:
            return resolution
        
        segments = list(filter(lambda x: len(x.strip()) > 0, endpoint.split("/")))
        segment, remaining = segments[0], '/'.join(segments[1:])

        if drive_id is None:
            
            sub_site = await self._find_subsite(site_id, segment)
            if sub_site is not None:
                self._log(logging.INFO, f'Resolved {segment} as subsite ({sub_site["id"]})')
                resolution.append((segment, sub_site["id"], None, None))
                return await self._resolve_rec(sub_site["id"], remaining, resolution=resolution)
            
            drive = await self._find_drive(site_id, segment)
            if drive is None:
                raise SegmentException(segment)
            self._log(logging.INFO, f'Resolved {segment} as drive ({drive["id"]})')
            resolution.append((segment, None, drive["id"], None))
            return await self._resolve_rec(site_id, remaining, drive['id'], resolution=resolution)

        if spfile is None:
            target = await self._find_drive_object(site_id, drive_id, segment)
        else: # Can't be a file due to fn first check
            target = await self._find_folder_object(drive_id, spfile['id'], segment)

        if target is None:
            raise SegmentException(segment)

        is_file = SPFile.is_file(target)
        self._log(logging.INFO, f'Resolved {segment} as {"file" if is_file else "folder"} ({target["id"]})')
        resolution.append((segment, None, None, target))
        return await self._resolve_rec(site_id, remaining, drive_id, target, resolution)

    # Counting

    async def _count_rec(
        self,
        drive_id: Identifier,
        folder: SPFile
    ) -> int:
        children = await self.request(
            f'/drives/{drive_id}/items/{folder["id"]}/children?$filter=folder ne null'
        )
        counts = await asyncio.gather(*[
            self._count_rec(drive_id, folder)
            for folder in children['value']
        ])

        return sum(counts, folder.get('folder', {}).get('childCount'))

    # Download    

    async def _download_worker(self, queue: Queue) -> None:
        while True:
            item: Tuple[SPFile, str] | None = await queue.get()
            if item is None:
                queue.task_done()
                break
            spfile, folder = item
            file_path = f'{folder}/{spfile["name"]}'

            if await is_up_to_date(file_path, spfile):
                self._log(logging.DEBUG, 'File already to date, skipping')
                queue.task_done()
                self._track('Downloading', 'progress', 1)
                continue

            link = spfile['@microsoft.graph.downloadUrl']
            try:
                self._log(
                    logging.INFO,
                    f'Downloading file \'{spfile["name"]}\' ...',
                )
                body: bytes = await self._request_single(link)
            except TimeoutError:
                size = human_readable_size(spfile['size'])
                self._log(
                    logging.ERROR,
                    f'[bold red]Timeout after {self.timeout_window}s while downloading \'{file_path}\' ({size})[/bold red]',
                    extra={ 'markup': True }
                )
                self._log(
                    logging.ERROR,
                    'Increase the client timeout window to avoid such issue'
                )
                continue

            await mkdir(folder)
            async with aiofiles.open(file_path, 'wb') as fout:
                await fout.write(body)
            async with aiofiles.open(f'{file_path}.metadata', 'w') as fout:
                await fout.write(json.dumps(spfile))

            queue.task_done()
            self._track('Downloading', 'progress', 1)


    async def _download_rec(
        self,
        resolution: Resolution,
        relative_path: str,
        queue: Queue
    ):
        site_id, drive_id, folder = resolution
        
        folder_id = None if folder is None else folder['id']
        items, next_page = await self.list((site_id, drive_id, folder_id), False, False)
        files, sub_folders = partition(lambda f: SPFile.is_file(f), items)
        for file in files:
            await queue.put((file, relative_path))

        while next_page is not None:
            items, next_page = await self._list_next(next_page)

            files, next_sub_folders = partition(lambda f: SPFile.is_file(f), items)
            sub_folders.extend(next_sub_folders)
            for file in files:
                await queue.put((file, relative_path))
            if next_page is None:
                break

        tasks = [
            self._download_rec(
                (site_id, drive_id, sub_folder),
                f'{relative_path}/{sub_folder["name"]}',
                queue
            ) for sub_folder in sub_folders
        ]

        await asyncio.gather(*tasks)        


    async def _download(
        self,
        local_dir: str,
        resolution: Resolution,
        total: int
    ):
        _, _, spfile = resolution
        self._track('Downloading', 'start', total)

        # Download workers
        queue = Queue(self.queue_size)
        workers = [
            asyncio.create_task(self._download_worker(queue))
            for _ in range(self.worker_count)
        ]

        if SPFile.is_file(spfile): # A single file
            await queue.put((spfile, local_dir))
            for _ in workers:
                await queue.put(None)
            await asyncio.gather(*workers)
            return

        await self._download_rec(resolution, local_dir, queue)
        for _ in workers:
            await queue.put(None)
        
        await asyncio.gather(*workers)
        self._track('Downloading', 'end', None)
        

    # Interface implem

    async def request[T : AnyGraphResponse](
        self,
        endpoint: str | List[str],
        headers: Optional[Dict[str, str]] = None,
        filters: Dict[str, str] = None
    ) -> T:
        '''Performs a request against the given endpoint.
        
        Authentication is guaranteed to be valid during the request. If the given input is a list,
        will attempt to batch the request using the dedicated graph endpoint.'''
        if isinstance(endpoint, list):
            return await self._request_batch(endpoint, headers)
        return await self._request_single(endpoint, headers, filters)


    async def resolution(
        self,
        endpoint: str,
        site_id: Optional[Identifier] = None,
    ) -> List[Resolution]:
        endpoint, site_id = self._determine_site_id(endpoint, site_id)
        self._track('Resolving ...', 'start')

        result = await self._resolve_rec(
            site_id,
            endpoint,
            resolution=[("ROOT", site_id, None, None)]
        )
        
        self._track('Resolving ...', 'end')
        return result


    async def resolve(
        self,
        endpoint: str,
        site_id: Optional[Identifier] = None
    ) -> Resolution:
        resolution = await self.resolution(endpoint, site_id)
        if len(resolution) == 0:
            raise ResolutionException('Unable to resolve the given endpoint')
        resolution = list(reversed(resolution))
        
        site_id = None
        drive_id = None
        spfile = None 

        for step in resolution:
            if spfile is None and step[3] is not None:
                spfile = step[3]
            if drive_id is None and step[2] is not None:
                drive_id = step[2]
            if site_id is None and step[1] is not None:
                site_id = step[1]
                break
        
        return site_id, drive_id, spfile


    async def list(
        self,
        endpoint: str | Resolution,
        handle_pagination: bool = False,
        tracked: bool = False
    ) -> Tuple[List[SPFile], Optional[str]]:
        '''Same as list_all, except for the pagination which is not handled.'''
        if isinstance(endpoint, tuple):
            site_id, drive_id, folder_id = endpoint
        else:
            site_id, drive_id, folder_id = await self.resolve(endpoint)

        url = self._url((site_id, drive_id, folder_id))
        total = (await self.count((site_id, drive_id, folder_id))) if tracked else None
        self._track('Listing ...', 'start', total, tracked)

        page_size = self.page_size
        response: SPResponse = await self.request(f'{url}/children?$top={page_size}')
        data = response.get('value', [])
        self._track('Listing ...', 'progress', len(data), tracked)
        next_url = response.get('@odata.nextLink')

        if not handle_pagination:
            return data, next_url

        # else
        while next_url is not None:
            response = await self.request(next_url)
            next_url = response.get('@odata.nextLink')
            page_data = response.get('value', [])
            self._track('Listing ...', 'progress', len(page_data), tracked)
            data.extend(page_data)

        self._track('Listing ...', 'end', tracked)
        return data, None


    async def _list_next(self, url: str) -> Tuple[List[SPFile], Optional[str]]:
        response: SPResponse = await self.request(url)
        return response.get('value', []), response.get('@odata.nextLink')


    @alru_cache(maxsize=32768) # 32k ints should'nt weight too much
    async def count(self, endpoint: str | Resolution) -> int:
        if isinstance(endpoint, tuple):
            site_id, drive_id, folder_id = endpoint
        else:
            site_id, drive_id, folder_id = await self.resolve(endpoint)
        
        self._track('Evaluating workload ...', 'start')

        url = self._url((site_id, drive_id, folder_id))
        root = await self.request(url)
        count = await self._count_rec(drive_id, root)
        
        self._track('Evaluating workload ...', 'end')
        return count


    async def download(
        self,
        endpoint: str | Resolution,
        local_dir: Optional[str] = None
    ):
        if isinstance(endpoint, tuple):
            site_id, drive_id, spfile = endpoint
        else:
            site_id, drive_id, spfile = await self.resolve(endpoint)

        folder_id = None
        if isinstance(spfile, dict):
            folder_id = spfile['id'] if SPFile.is_folder(spfile) else None
        total = await self.count((site_id, drive_id, folder_id))
        
        local_dir = resolve_local_dir(endpoint, local_dir) # FIXME endpoint may be a tuple
        
        await self._download(local_dir, (site_id, drive_id, spfile), total)
