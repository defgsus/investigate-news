import datetime
import json
import os
import sys
import gzip
from io import TextIOWrapper
from pathlib import Path
import hashlib
from typing import Optional, Generator

import requests
from decouple import config
from tqdm import tqdm


class TMDB:

    CACHE_DIR = Path(__file__).resolve().parent.parent.parent / "cache" / "tmdb"
    API_HOST = "https://api.themoviedb.org"

    def __init__(self, verbose: bool = True):
        self.verbose = verbose

        token = config("TMDB_ACCESS_TOKEN", cast=str)
        self._session = requests.Session()
        self._session.headers = {
            "Authorization": f"Bearer {token}"
        }

    def get_movie(self, id: int) -> dict:
        return self.get_json(f"3/movie/{id}", params={"append_to_response": "credits"})

    def iter_movie_ids(self, date: datetime.date, adult: bool = False) -> Generator[dict, None, None]:
        filename = self.get_file(f"p/exports/{'adult_' if adult else ''}movie_ids_{date.month:02}_{date.day:02}_{date.year:04}.json.gz")

        yield from self._iter_ndjson_gz(filename)

    def get_json(self, url: str, params: Optional[dict] = None) -> dict:
        hash_code = hashlib.sha384(f"{url}-{params}".encode()).hexdigest()
        cache_filename = self.CACHE_DIR / hash_code[:2] / f"{hash_code}.json"

        if cache_filename.exists():
            return json.loads(cache_filename.read_text())

        os.makedirs(cache_filename.parent, exist_ok=True)

        if self.verbose:
            print(f"requesting {url} {params}", file=sys.stderr)

        response = self._session.get(
            f"{self.API_HOST}/{url}",
            params=params,
        )
        if response.status_code != 200:
            raise RuntimeError(
                f"Got response {response.status_code} for {url} / {params}"
            )

        response = response.json()
        cache_filename.write_text(json.dumps(response))

        return response

    def get_file(self, url: str) -> Path:
        cache_filename = self.CACHE_DIR / url
        if cache_filename.exists():
            return cache_filename

        url = f"https://files.tmdb.org/{url}"
        response = self._session.get(url, stream=True)

        os.makedirs(cache_filename.parent, exist_ok=True)
        with cache_filename.open("wb") as fp:
            for chunk in tqdm(response.iter_content(chunk_size=2^16), disable=not self.verbose, desc=f"downloading {url}"):
                fp.write(chunk)

        return cache_filename

    def _iter_ndjson_gz(self, filename):
        with gzip.open(filename) as fp:
            fp_text = TextIOWrapper(fp)
            for line in fp_text:
                yield json.loads(line)
