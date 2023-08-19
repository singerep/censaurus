from typing import Dict, List, Tuple, Iterable
from httpx import AsyncClient, Timeout, Response, ConnectTimeout, ConnectError, ReadTimeout, PoolTimeout
from asyncio import new_event_loop, set_event_loop, run_coroutine_threadsafe, sleep, gather
from threading import Thread


class CensusAPIKeyError(Exception):
    def __init__(self) -> None:
        super().__init__('Looks like you are missing an API key! You can obtain one at https://api.census.gov/data/key_signup.html. You can set it by passing it into the api_key parameter.')


class CensusAPIError(Exception):
    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(f'The Census API had an error (status code {status_code}) and returned the following message:\n\n{message}')
        self.status_code = status_code
        self.message = message


class TIGERWebAPIError(Exception):
    def __init__(self, status_code: int, message: str) -> None:
        if status_code:
            super().__init__(f'The TIGER API had an error (status code {status_code}) and returned the following message:\n\n{message}')
        else:
            super().__init__(message)

        self.status_code = status_code
        self.message = message


class AsyncLoopHandler(Thread):
    """
    Class to handle asynchronous requests. Useful especially in the case where a user
    is writing code inside an IPython environment.

    Adapted from https://stackoverflow.com/a/66055205/17834461
    """
    def __init__(self):
        super().__init__(daemon=True)
        self.loop = new_event_loop()

    def run(self):
        set_event_loop(self.loop)
        self.loop.run_forever()
        return self.loop


class CensusClient(AsyncClient):
    """
    An object that interfaces with the Census API. Extends the 
    :class:`httpx.AsyncClient`` class to allow for asynchronous requests.

    Parameters
    ==========
    url_extension : :obj:`str`
        A URL extension to append to ``https://api.census.gov/data/`` for accessing
        data related to a specific :class:`.Dataset`.
    api_key : :obj:`str` = None
        A Census API key. Can be obtained
        `here <https://api.census.gov/data/key_signup.html>`_. Not necessary unless you
        are making a large number of calls.
    """
    def __init__(self, url_extension: str, api_key: str = None, **kwargs):
        timeout = Timeout(30.0, connect=30.0)
        super().__init__(timeout=timeout)

        self.root = f'https://api.census.gov/data/{url_extension}'
        self.api_key = api_key
        self.chunk_size = 50
        self.retry_limit = kwargs.pop('retry_limit', 2)

        self._loop_handler = AsyncLoopHandler()
        self._loop_handler.start()

        self.response = None

    def get_sync(self, url: str = '', params: Dict[str, str] = {}) -> Response:
        """
        Make a single request to the Census API synchronously.

        Parameters
        ==========
        url : :obj:`str` = ''
            The relative URL to request.
        params : :obj:`dict` of :obj:`str`: :obj:`str`
            Query parameters to supply to the Census API.
        """
        future = run_coroutine_threadsafe(self.get(url=url, params=params), self._loop_handler.loop)
        return future.result()

    def get_many_sync(self, url_params_list: Iterable[Tuple[str, Dict[str, str]]] = []) -> List[Response]:
        """
        Make a more than one request to the Census API synchronously. Note that while
        the requests are still sent asynchronously, the function call itself is 
        synchronous.

        Parameters
        ==========
        url_params_list : array-like of :obj:`tuple` of :obj:`str` and :obj:`dict` of :obj:`str`: :obj:`str`
            An array-like of tuples, where each tuple consists of a URL to request and a
            set of query parameters to supply to the Census API.
        """
        future = run_coroutine_threadsafe(self.get_many(url_params_list=url_params_list), self._loop_handler.loop)
        return future.result()

    async def get(self, url: str = '', params: Dict[str, str] = {}) -> Response:
        """
        Make a single request to the Census API asynchronously.

        Parameters
        ==========
        url : :obj:`str` = ''
            The relative URL to request.
        params : :obj:`dict` of :obj:`str`: :obj:`str`
            Query parameters to supply to the Census API.
        """
        if self.api_key is not None:
            params.update({'key', self.api_key})
        url = self.root + url

        retry_count = 0
        while self.retry_limit is None or retry_count < self.retry_limit:
            retry_count += 1
            try:
                response = await super().get(url=url, params=params)
            except (ConnectTimeout, ConnectError, ReadTimeout, PoolTimeout):
                response = None

            if response is None:
                await sleep(2)
                continue

            if response.status_code == 200 or response.status_code == 204:
                return response

            if response.status_code == 404:
                break

        raise CensusAPIError(status_code=response.status_code, message=response.text)

    async def get_many(self, url_params_list: Iterable[Tuple[str, Dict[str, str]]] = []) -> List[Response]:
        """
        Make a more than one request to the Census API asynchronously.

        Parameters
        ==========
        url_params_list : array-like of :obj:`tuple` of :obj:`str` and :obj:`dict` of :obj:`str`: :obj:`str`
            An array-like of tuples, where each tuple consists of a URL to request and a set 
            of query parameters to supply to the Census API.
        """
        tasks = []
        for url, params in url_params_list:
            tasks.append(self._loop_handler.loop.create_task(self.get(url, params=params)))

        chunks = [tasks[i:i + self.chunk_size] for i in range(0, len(tasks), self.chunk_size)]
        responses = []
        for chunk in chunks:
            chunk_responses = await gather(*chunk)
            responses.extend(chunk_responses)
        
        return responses


class TIGERClient(AsyncClient):
    """
    An object that interfaces with the TIGERWeb API. Extends the 
    :class:`httpx.AsyncClient`` class to allow for asynchronous requests.

    Parameters
    ==========
    map_service : :obj:`str` = 'tigerWMS_Current'
        The TIGERWeb MapService to use as the basis for this client. Defaults to the
        current map service.
    """
    def __init__(self, map_service: str = 'tigerWMS_Current', **kwargs):
        timeout = Timeout(30.0, connect=30.0)
        super().__init__(base_url=f'https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/{map_service}/MapServer', timeout=timeout)
        self.chunk_size = 100
        self.retry_limit = kwargs.pop('retry_limit', 2)

        self._loop_handler = AsyncLoopHandler()
        self._loop_handler.start()

    def get_sync(self, url: str = '', params: Dict[str, str] = {}, return_type: str = 'json') -> Response:
        """
        Make a single request to the TIGERWeb API synchronously.

        Parameters
        ==========
        url : :obj:`str` = ''
            The relative URL to request.
        params : :obj:`dict` of :obj:`str`: :obj:`str`
            Query parameters to supply to the TIGERWeb API.
        return_type : :obj:`str` = 'json'
            Determines the type of data to return. Should either be ``json`` or
            ``geojson``.
        """
        future = run_coroutine_threadsafe(self.get(url=url, params=params, return_type=return_type), self._loop_handler.loop)
        return future.result()

    def get_many_sync(self, url_params_list: List[Tuple[str, Dict[str, str]]] = [], return_type = 'json') -> List[Response]:
        """
        Make a more than one request to the TIGERWeb API synchronously. Note that while
        the requests are still sent asynchronously, the function call itself is 
        synchronous.

        Parameters
        ==========
        url_params_list : array-like of :obj:`tuple` of :obj:`str` and :obj:`dict` of :obj:`str`: :obj:`str`
            An array-like of tuples, where each tuple consists of a URL to request and a
            set of query parameters to supply to the TIGERWeb API.
        """
        future = run_coroutine_threadsafe(self.get_many(url_params_list=url_params_list, return_type=return_type), self._loop_handler.loop)
        return future.result()

    async def get(self, url: str = '', params: Dict[str, str] = {}, return_type = 'json') -> Response:
        """
        Make a single request to the TIGERWeb API asynchronously.

        Parameters
        ==========
        url : :obj:`str` = ''
            The relative URL to request.
        params : :obj:`dict` of :obj:`str`: :obj:`str`
            Query parameters to supply to the TIGERWeb API.
        return_type : :obj:`str` = 'json'
            Determines the type of data to return. Should either be ``json`` or
            ``geojson``.
        """
        params.update({'f': return_type})
        retry_count = 0
        while self.retry_limit is None or retry_count < self.retry_limit:
            retry_count += 1
            try:
                response = await super().get(url=url, params=params)
            except (ConnectTimeout, ConnectError, ReadTimeout, PoolTimeout):
                response = None

            if response is None:
                await sleep(2)
                continue

            if 'The requested URL was rejected' in response.text:
                raise TIGERWebAPIError(200, 'The requested URL was rejected.')
            if 'Invalid URL' in response.text:
                raise TIGERWebAPIError(400, 'Invalid URL.')
            if 'Error performing query operation' in response.text or 'Failed to execute query' in response.text:
                raise TIGERWebAPIError(500, 'Error performing query operation.')

            if response.status_code == 200:
                return response

        raise TIGERWebAPIError(status_code=None, message='Your TIGERWeb request failed for an unknown reason.')

    async def get_many(self, url_params_list: Iterable[Tuple[str, Dict[str, str]]] = [], return_type = 'json') -> List[Response]:
        """
        Make a more than one request to the TIGERWeb API asynchronously.

        Parameters
        ==========
        url_params_list : array-like of :obj:`tuple` of :obj:`str` and :obj:`dict` of :obj:`str`: :obj:`str`
            An array-like of tuples, where each tuple consists of a URL to request and a
            set of query parameters to supply to the TIGERWeb API.
        """
        tasks = []
        for url, params in url_params_list:
            tasks.append(self._loop_handler.loop.create_task(self.get(url, params=params, return_type=return_type)))

        chunks = [tasks[i:i + self.chunk_size] for i in range(0, len(tasks), self.chunk_size)]
        responses = []
        for chunk in chunks:
            chunk_responses = await gather(*chunk)
            responses.extend(chunk_responses)
        
        return responses