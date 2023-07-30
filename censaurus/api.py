from typing import Dict, List, Tuple

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

class TIGERError(Exception):
    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(f'The Census API had an error (status code {status_code}) and returned the following message:\n\n{message}')


class CensusClient(httpx.AsyncClient):
    def __init__(self, url_extension: str, api_key: str = None, **kwargs):
        timeout = httpx.Timeout(30.0, connect=30.0)
        super().__init__(timeout=timeout)

        self.root = f'https://api.census.gov/data/{url_extension}'
        self.api_key = api_key
        self.chunk_size = 50
        self.retry_limit = kwargs.pop('retry_limit', 5)

    def get_sync(self, url: str = '', params: Dict[str, str] = {}) -> httpx.Response:
        return loop.run_until_complete(self.get(url=url, params=params))

    def get_many_sync(self, url_params_list: List[Tuple[str, Dict[str, str]]] = []) -> List[httpx.Response]:
        return loop.run_until_complete(self.get_many(url_params_list=url_params_list))

    async def get(self, url: str = '', params: Dict[str, str] = {}) -> httpx.Response:
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
                # self.logger.log(message=f'httpx error; trying again')
                await asyncio.sleep(2)
                continue

            if response.status_code == 200 or response.status_code == 204:
                return response

            # TODO: figure out what missing api key status code is
            if response.status_code == 0:
                raise CensusAPIKeyError()
            
        raise CensusAPIError(status_code=response.status_code, message=response.text)

    async def get_many(self, url_params_list: List[Tuple[str, Dict[str, str]]] = []) -> List[httpx.Response]:
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
    def __init__(self, map_service: str = 'TIGERweb/tigerWMS_Current', **kwargs):
        timeout = httpx.Timeout(30.0, connect=30.0)
        super().__init__(base_url=f'https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/{map_service}/MapServer', timeout=timeout)
        
        self.chunk_size = 100
        self.retry_limit = kwargs.pop('retry_limit', 5)

    def get_sync(self, url: str = '', params: Dict[str, str] = {}, return_type = 'json') -> httpx.Response:
        return loop.run_until_complete(self.get(url=url, params=params, return_type=return_type))

    def get_many_sync(self, url_params_list: List[Tuple[str, Dict[str, str]]] = [], return_type = 'json') -> List[httpx.Response]:
        return loop.run_until_complete(self.get_many(url_params_list=url_params_list, return_type=return_type))

    async def get(self, url: str = '', params: Dict[str, str] = {}, return_type = 'json') -> httpx.Response:
        params.update({'f': return_type})
        response = await super().get(url=url, params=params)

        retry_count = 0
        while self.retry_limit is None or retry_count < self.retry_limit:
            retry_count += 1
            try:
                response = await super().get(url=url, params=params)
            except (httpx.ConnectTimeout, httpx.ConnectError, httpx.ReadTimeout, httpx.PoolTimeout):
                response = None

            if response is None:
                # self.logger.log(message=f'httpx error; trying again')
                await asyncio.sleep(2)
                continue

            if response.status_code == 200:
                return response
            
        raise TIGERError(status_code=response.status_code, message=response.text)

    async def get_many(self, url_params_list: List[Tuple[str, Dict[str, str]]] = [], return_type = 'json') -> List[httpx.Response]:
        tasks = []
        for url, params in url_params_list:
            tasks.append(loop.create_task(self.get(url, params=params, return_type=return_type)))

        chunks = [tasks[i:i + self.chunk_size] for i in range(0, len(tasks), self.chunk_size)]
        responses = []
        for chunk in chunks:
            chunk_responses = await asyncio.gather(*chunk)
            responses.extend(chunk_responses)
        
        return responses