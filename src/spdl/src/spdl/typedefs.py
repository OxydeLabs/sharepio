from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, TypeAlias, TypedDict, Literal, Callable, Self
from aiohttp import ClientResponseError, RequestInfo, ClientResponse
from aiohttp.typedefs import LooseHeaders

# Identification

Identifier: TypeAlias = str


_SPFile = TypedDict('SPFile', {
    '@microsoft.graph.downloadUrl': Optional[str]
})


class SPFile(_SPFile):
    
    id: Identifier
    name: str
    file: Optional[Dict[str, Any]]
    folder: Optional[Dict[str, Any]]
    size: int

    @staticmethod
    def is_file(spfile):
        return spfile is not None and 'file' in spfile
    
    @staticmethod
    def is_folder(spfile):
        return spfile is not None and 'folder' in spfile


Resolution: TypeAlias = Tuple[
    Optional[Identifier],
    Optional[Identifier],
    Optional[Identifier | SPFile]
]

Resolvable: str | Resolution


SPResponse = TypedDict('SPResponse', {
    'value': Optional[List[SPFile]],
    '@odata.nextLink': Optional[str],
    '@odata.count': Optional[int]
})

TrackingStep: TypeAlias = Literal['start', 'progress', 'end']
Tracker: TypeAlias = Callable[[str, TrackingStep, int], None]
'''A function to be called when progress on a task, whatever it may be, has occured.

The function is called first with a ``start`` tracking step, and a total iff a total is known,
None otherwise. Subsequent calls will call the function with a ``progress`` step, and a count
representing the advance taken toward the task completion (min 1). Eventually the function is called
with an ``end`` step, signifying that the task is complete. Note that some tasks may skip the
``progress`` steps if the given task is atomic.'''


class AuthToken(TypedDict):
    token_type: str
    access_token: str
    expires_in: int
    expires_at: datetime


# Config

class ServerConfig(TypedDict):
    tenant_id: str
    proxy_host: str
    proxy_port: int
    ca_cert_path: str


class ClientConfig(TypedDict):
    chunk_size: int
    retry_count: int
    worker_count: int
    logging_level: Optional[int]
    logging_files: Optional[List[str]]


class AuthConfig(TypedDict):
    client_id: str
    private_key_path: str
    cert_path: str
    cert_thumbprint: str
    scopes: List[str]
    safety_delta: int = 180


class Config(TypedDict):
    server: ServerConfig
    client: ClientConfig
    auth: AuthConfig
    sites: Dict[str, str]
    token: Optional[AuthToken]


# Exceptions

class ApiException(Exception):
    
    def __init__(self, http_code: int, msg: str, detail: Dict[any, any] = None) -> None:
        super().__init__(msg)
        self.http_code = http_code
        self.detail = detail or {}


class SegmentException(Exception):
    def __init__(self, erroneous_segment: str) -> None:
        super().__init__(erroneous_segment)
        self.erroneous_segment = erroneous_segment


class ConfigException(Exception):

    def __init__(self, config_key: str, config_value: Optional[any]):
        # TODO RESOLVE subconfigs
        expected_type = vars(Config)['__annotation__'].get(config_key).__name__
        message = f'Incorrect configuration for \'{config_key}\', expected {expected_type} but was \'{str(config_value)}\''
        super(ConfigException, self).__init__(message)


class ThrottleException(ApiException):
    '''Exception thrown on 429 API responses'''

    throttle_time: int
    
    def __init__(self, throttle_time: int) -> None:
        super().__init__(429, 'Throttle limit reached')
        self.throttle_time = throttle_time


class ResolutionException(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class DetailedClientResponseError(ClientResponseError):
    
    def __init__(
        self,
        request_info: RequestInfo,
        history: Tuple[ClientResponse, ...],
        *,
        code: Optional[int] = None,
        status: Optional[int] = None,
        message: str = "",
        headers: Optional[LooseHeaders] = None,
        body: Any = None
    ) -> None:
        super().__init__(request_info, history, status=status, message=message, headers=headers)
        self.body = body

    @classmethod
    def wrap(cls, cre: ClientResponseError, body: Any) -> Self:
        return cls(
            cre.request_info,
            cre.history,
            code=cre.code,
            status=cre.status,
            message=cre.message,
            headers=cre.headers,
            body=body
        )