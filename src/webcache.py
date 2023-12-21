import os
import sys
import hashlib
import pickle
from pathlib import Path
from typing import Optional, Union

import requests


class WebCache:

    def __init__(
            self,
            path: Union[str, Path],
            headers: Optional[dict] = None,
            timeout: int = 5,
            verbose: bool = False,
    ):
        import plyvel

        self.path = Path(path)
        self.timeout = timeout
        self.verbose = verbose
        self.session = requests.Session()
        self.session.headers = {
            "user-agent": "github.com/defgsus/sponso",
            **(headers or {}),
        }
        self.num_requests = 0

        self._db: Optional[plyvel.DB] = None

    def close(self):
        if self._db:
            self._db.close()

    @property
    def db(self):
        import plyvel

        if self._db is None:
            os.makedirs(self.path, exist_ok=True)
            self._db = plyvel.DB(str(self.path), create_if_missing=True)
        return self._db

    def request(self, method: str, url: str, **kwargs) -> requests.Response:
        cache_key = hashlib.sha384(f"{method} {url} {kwargs} {self.session.headers}".encode()).hexdigest().encode()
        cache_entry = self.db.get(cache_key)

        if cache_entry is not None:
            return pickle.loads(cache_entry)

        kwargs.setdefault("timeout", self.timeout)
        if self.verbose:
            print(f"requesting {method} {url} {kwargs}", file=sys.stderr)
        response = self.session.request(method, url, **kwargs)
        self.num_requests += 1

        self.db.put(cache_key, pickle.dumps(response))

        return response

    def get(self, url, **kwargs):
        return self.request("GET", url, **kwargs)
