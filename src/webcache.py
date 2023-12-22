import os
import sys
import hashlib
import pickle
import time
from pathlib import Path
from typing import Optional, Union

import requests


class WebCache:

    def __init__(
            self,
            path: Union[str, Path],
            headers: Optional[dict] = None,
            default_timeout: float = 10.,
            verbose: bool = True,
            requests_per_second: Optional[float] = None,
            cache_mode: str = "rw",
    ):
        import plyvel
        assert cache_mode in ("r", "w", "rw"), cache_mode

        self.path = Path(path)
        self.default_timeout = default_timeout
        self.verbose = verbose
        self.requests_per_second = requests_per_second
        self.cache_mode = cache_mode
        self._last_request_time = 0
        self.session = requests.Session()
        self.session.headers = {
            "user-agent": "github.com/defgsus/investigate-news",
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

    def request(
            self,
            method: str,
            url: str,
            stream: bool = False,
            timeout: Optional[float] = None,
            cache_mode: Optional[str] = None,
            **kwargs,
    ) -> requests.Response:
        if stream:
            raise ValueError(f"Sorry, stream=True is not supported in WebCache")

        cache_mode = cache_mode or self.cache_mode

        cache_key = hashlib.sha384(f"{method} {url} {kwargs} {self.session.headers}".encode()).hexdigest().encode()

        if "r" in cache_mode:
            cache_entry = self.db.get(cache_key)

            if cache_entry is not None:
                return pickle.loads(cache_entry)

        if self.requests_per_second is not None:
            wait_time = 1. / self.requests_per_second
            cur_time = time.time()
            time_passed = cur_time - self._last_request_time
            # print("wait_time", wait_time, time_passed)
            if time_passed < wait_time:
                wait_time -= time_passed
                # print("waiting", wait_time)
                time.sleep(wait_time)

            self._last_request_time = time.time()

        if self.verbose:
            print(f"requesting {method} {url} {kwargs}", file=sys.stderr)

        response = self.session.request(method, url, timeout=timeout or self.default_timeout, **kwargs)
        self.num_requests += 1

        if "w" in self.cache_mode:
            self.db.put(cache_key, pickle.dumps(response))

        return response

    def get(self, url, **kwargs):
        return self.request("GET", url, **kwargs)
