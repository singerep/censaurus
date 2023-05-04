import requests

class Connection:
    def __init__(self, url) -> None:
        self.url = url
        self.response = requests.get(url=url)