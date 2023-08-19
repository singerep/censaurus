from unittest import TestCase, main
from httpx import Response
from typing import List

from censaurus.api import CensusClient, TIGERClient, CensusAPIError, TIGERWebAPIError


class APITest(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.census_client = CensusClient(url_extension='2021/acs/acs5')
        cls.tiger_client = TIGERClient()

    def test_census_client(self):
        valid_urls = ['/geography.json', '/variables.json']
        for url in valid_urls:
            try:
                response = self.census_client.get_sync(url=url)
                self.assertIsInstance(response, Response)
            except CensusAPIError as e:
                self.assertTrue(e.status_code != 404)
            except Exception as e:
                self.fail('Unexpected exception.')

        try:
            valid_url_params_list = list(zip(valid_urls, [{}]*len(valid_urls)))
            responses = self.census_client.get_many_sync(url_params_list=valid_url_params_list)
            self.assertIsInstance(responses, List)
        except CensusAPIError as e:
            self.assertTrue(e.status_code != 404)
        except Exception as e:
            self.fail('Unexpected exception.')

        with self.assertRaises(CensusAPIError) as context:
            response = self.census_client.get_sync(url='bad_url')
        self.assertTrue(context.exception.status_code == 404)

    def test_tiger_client(self):
        valid_urls = ['/80', '/82']
        for url in valid_urls:
            try:
                response = self.tiger_client.get_sync(url=url)
                self.assertIsInstance(response, Response)
            except TIGERWebAPIError as e:
                self.assertTrue(e.status_code != 404)
            except Exception as e:
                self.fail('Unexpected exception.')

        try:
            valid_url_params_list = list(zip(valid_urls, [{}]*len(valid_urls)))
            responses = self.tiger_client.get_many_sync(url_params_list=valid_url_params_list)
            self.assertIsInstance(responses, List)
        except TIGERWebAPIError as e:
            self.assertTrue(e.status_code != 404)
        except Exception as e:
            self.fail('Unexpected exception.')

        with self.assertRaises(TIGERWebAPIError) as context:
            response = self.tiger_client.get_sync(url='/82/query', params={'where': 'bad_param=bad_value'})
        self.assertTrue(context.exception.status_code == 500, context.exception.status_code)


if __name__ == "__main__":
    main()