from typing import Dict, List, Tuple, Iterable
import httpx
import asyncio

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)


class CensusAPIKeyError(Exception):
    def __init__(self) -> None:
        super().__init__('Looks like you are missing an API key! You can obtain one at https://api.census.gov/data/key_signup.html. You can set it by passing it into the api_key parameter.')


class CensusAPIError(Exception):
    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(f'The Census API had an error (status code {status_code}) and returned the following message:\n\n{message}')


class TIGERWebError(Exception):
    ...


class TIGERWebAPIError(Exception):
    def __init__(self, status_code: int, message: str) -> None:
        if status_code:
            super().__init__(f'The TIGER API had an error (status code {status_code}) and returned the following message:\n\n{message}')
        else:
            super().__init__(message)


class CensusClient(httpx.AsyncClient):
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
        timeout = httpx.Timeout(30.0, connect=30.0)
        super().__init__(timeout=timeout)

        self.root = f'https://api.census.gov/data/{url_extension}'
        self.api_key = api_key
        self.chunk_size = 50
        self.retry_limit = kwargs.pop('retry_limit', 2)

    def get_sync(self, url: str = '', params: Dict[str, str] = {}) -> httpx.Response:
        """
        Make a single request to the Census API synchronously.

        Parameters
        ==========
        url : :obj:`str` = ''
            The relative URL to request.
        params : :obj:`dict` of :obj:`str`: :obj:`str`
            Query parameters to supply to the Census API.
        """
        return loop.run_until_complete(self.get(url=url, params=params))

    def get_many_sync(self, url_params_list: Iterable[Tuple[str, Dict[str, str]]] = []) -> List[httpx.Response]:
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
        return loop.run_until_complete(self.get_many(url_params_list=url_params_list))

    async def get(self, url: str = '', params: Dict[str, str] = {}) -> httpx.Response:
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
            except (httpx.ConnectTimeout, httpx.ConnectError, httpx.ReadTimeout, httpx.PoolTimeout):
                response = None

            if response is None:
                await asyncio.sleep(2)
                continue

            if response.status_code == 200 or response.status_code == 204:
                return response

            if response.status_code == 0:
                raise CensusAPIKeyError()

        raise CensusAPIError(status_code=response.status_code, message=response.text)

    async def get_many(self, url_params_list: Iterable[Tuple[str, Dict[str, str]]] = []) -> List[httpx.Response]:
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
            tasks.append(loop.create_task(self.get(url, params=params)))

        chunks = [tasks[i:i + self.chunk_size] for i in range(0, len(tasks), self.chunk_size)]
        responses = []
        for chunk in chunks:
            chunk_responses = await asyncio.gather(*chunk)
            responses.extend(chunk_responses)
        
        return responses


class TIGERClient(httpx.AsyncClient):
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
        timeout = httpx.Timeout(30.0, connect=30.0)
        super().__init__(base_url=f'https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/{map_service}/MapServer', timeout=timeout)
        self.chunk_size = 100
        self.retry_limit = kwargs.pop('retry_limit', 2)

    def get_sync(self, url: str = '', params: Dict[str, str] = {}, return_type: str = 'json') -> httpx.Response:
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
        return loop.run_until_complete(self.get(url=url, params=params, return_type=return_type))

    def get_many_sync(self, url_params_list: List[Tuple[str, Dict[str, str]]] = [], return_type = 'json') -> List[httpx.Response]:
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
        return loop.run_until_complete(self.get_many(url_params_list=url_params_list, return_type=return_type))

    async def get(self, url: str = '', params: Dict[str, str] = {}, return_type = 'json') -> httpx.Response:
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
            except (httpx.ConnectTimeout, httpx.ConnectError, httpx.ReadTimeout, httpx.PoolTimeout):
                response = None

            if response is None:
                await asyncio.sleep(2)
                continue

            if 'The requested URL was rejected' in response.text:
                raise TIGERWebAPIError(200, 'The requested URL was rejected.')
            if 'Invalid URL' in response.text:
                raise TIGERWebAPIError(400, 'Invalid URL.')
            if 'Error performing query operation' in response.text:
                raise TIGERWebAPIError(500, 'Error performing query operation.')

            if response.status_code == 200:
                return response

        raise TIGERWebAPIError(status_code=None, message='Your TIGERWeb request failed.')

    async def get_many(self, url_params_list: Iterable[Tuple[str, Dict[str, str]]] = [], return_type = 'json') -> List[httpx.Response]:
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
            tasks.append(loop.create_task(self.get(url, params=params, return_type=return_type)))

        chunks = [tasks[i:i + self.chunk_size] for i in range(0, len(tasks), self.chunk_size)]
        responses = []
        for chunk in chunks:
            chunk_responses = await asyncio.gather(*chunk)
            responses.extend(chunk_responses)
        
        return responses