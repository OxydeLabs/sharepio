
#!/usr/bin/env python

from abc import ABCMeta
from typing import List, Tuple, TypeAlias, Optional, Dict

from aiohttp import ClientResponse

from ..typedefs import SPResponse, Identifier, SPFile, Resolution

__all__ = ['GraphResponse', 'BatchGraphResponse', 'AnyGraphResponse', 'Client' ]

GraphResponse: TypeAlias = bytes | SPResponse
BatchGraphResponse: TypeAlias = Tuple[List[SPResponse], List[ClientResponse]]
AnyGraphResponse: TypeAlias = GraphResponse | BatchGraphResponse


class Client(metaclass=ABCMeta):

    def request[T : AnyGraphResponse](
        self,
        endpoint: str | List[str],
        headers: Optional[Dict[str, str]] = None,
        filters: Optional[Dict[str, str]] = None
    ) -> T: ...

    def resolution(
        self,
        endpoint: str,
        site_id: Optional[Identifier] = None,
    ) -> List[Resolution]: ...

    def resolve(
        self,
        endpoint: str,
        site_id: Optional[Identifier] = None
    ) -> Resolution: ...

    def list(
        self,
        endpoint: str | Resolution,
        handle_pagination: bool = False,
        tracked: bool = False
    ) -> Tuple[List[SPFile], Optional[str]]: ...

    def count(self, endpoint: str | Resolution) -> int: ...

    def download(
        self,
        endpoint: str | Resolution,
        local_dir: Optional[str] = None,
    ) -> None: ...
