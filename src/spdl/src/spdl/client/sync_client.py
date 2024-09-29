#!/usr/bin/env python


from asyncio import Runner
from typing import Dict, List, Optional, Self, Tuple

from rich.console import Console

from ..typedefs import Config, SPFile, Tracker, Identifier, Resolution
from .async_client import AsyncClient
from .client import AnyGraphResponse, Client


__all__ = ['SyncClient']


class SyncClient(Client):
    
    _delegate: AsyncClient
    _runner: Runner

    # Ctor

    def __init__(
        self,
        config: Config,
        tracker: Optional[Tracker] = None,
        console: Optional[Console] = None,
        runner: Optional[Runner] = None
    ) -> None:
        self._delegate = AsyncClient(config, tracker, console)
        self._runner = Runner() if runner is None else runner

    # Ctx

    def __enter__(self) -> Self:
        self._runner._lazy_init()
        self._delegate.start()
        return self


    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._runner.run(self._delegate.close())
        self._runner.close()

    # Methods

    def request[T : AnyGraphResponse](
        self,
        endpoint: str | List[str],
        headers: Optional[Dict[str, str]] = None,
        filters: Optional[Dict[str, str]] = None
    ) -> T:
        return self._runner.run(self._delegate.request(endpoint, headers, filters))


    def resolution(
        self,
        endpoint: str,
        site_id: Optional[Identifier] = None,
    ) -> List[Resolution]:
        return self._runner.run(self._delegate.resolution(endpoint, site_id))
    

    def resolve(
        self,
        endpoint: str,
        site_id: Optional[Identifier] = None
    ) -> Resolution:
        return self._runner.run(self._delegate.resolve(endpoint, site_id))


    def list(
        self,
        endpoint: str | Resolution,
        handle_pagination: bool = False,
        tracked: bool = False
    ) -> Tuple[List[SPFile], Optional[str]]:
        return self._runner.run(self._delegate.list(endpoint, handle_pagination, tracked))


    def count(self, endpoint: str | Resolution) -> int:
        return self._runner.run(self._delegate.count(endpoint))


    def download(
        self,
        endpoint: str | Resolution,
        local_dir: Optional[str] = None,
    ) -> None:
        self._runner.run(self._delegate.download(endpoint, local_dir))
