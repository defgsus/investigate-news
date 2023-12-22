import json
import sys
import time
from pathlib import Path
import xml.etree.ElementTree as ET
from typing import Union, Optional

import requests
from tqdm import tqdm

from src.webcache import WebCache


class Arxiv:

    def __init__(
            self,
            db_path: Union[str, Path] = Path(__file__).resolve().parent.parent.parent / "cache/arxiv/db",
            cache: Union[str, Path, WebCache] = Path(__file__).resolve().parent.parent.parent / "cache/arxiv/web",
            verbose: bool = True,
    ):
        self.db_path = Path(db_path)
        if isinstance(cache, WebCache):
            self.cache = cache
        else:
            self.cache = WebCache(path=cache, requests_per_second=1. / 3.5)
        self.verbose = verbose
        self._db = None

    @property
    def db(self):
        if self._db is None:
            import plyvel
            self._db = plyvel.DB(str(self.db_path), create_if_missing=True)
        return self._db

    def query(
            self,
            query: str,
            sort_by: 'Literal["relevance", "lastUpdatedDate", "submittedDate"]' = "submittedDate",
            sort_order: 'Literal["ascending", "descending"]' = "ascending",
            page_size: int = 100,
            store: bool = False,
    ):
        result = None
        start = 0
        with tqdm(desc="query-pages", disable=not self.verbose) as progress:
            while True:
                page_result = self.query_paged(
                    query=query,
                    start=start,
                    max_results=page_size,
                    sort_by=sort_by,
                    sort_order=sort_order,
                    store=store
                )
                if not page_result.get("entry"):
                    if result is None:
                        result = page_result
                    break

                if result is None:
                    result = page_result
                else:
                    result["entry"].extend(page_result["entry"])

                progress.total = int(page_result["totalResults"]["text"])
                progress.update(len(page_result["entry"]))

                start += len(page_result["entry"])

        return result

    def query_paged(
            self,
            query: str,
            start: int = 0,
            max_results: int = 10,
            sort_by: 'Literal["relevance", "lastUpdatedDate", "submittedDate"]' = "submittedDate",
            sort_order: 'Literal["ascending", "descending"]' = "ascending",
            store: bool = False,
    ):
        cache_mode = "rw"
        while True:
            response = self.cache.get(
                "https://export.arxiv.org/api/query",
                params={
                    "search_query": query,
                    "start": start,
                    "max_results": max_results,
                    "sortBy": sort_by,
                    "sortOrder": sort_order,
                },
                cache_mode=cache_mode,
            )
            if response.status_code != 200:
                raise RuntimeError(f"Got status {response.status_code} from {response.request.url}")

            response = self._xml_to_json(response.text)

            if "entry" in response and not isinstance(response["entry"], list):
                response["entry"] = [response["entry"]]

            num_entries = len(response["entry"]) if response.get("entry") else 0
            total_entries = int(response["totalResults"]["text"])
            if start < total_entries and not num_entries:
                cache_mode = "w"
                if self.verbose:
                    print(f"Got empty response with totalResults={total_entries}, start={start}. Retrying in 5 sec..", file=sys.stderr)
                time.sleep(5.)
                # raise RuntimeError(f"Got empty response from arxiv.org:\n{json.dumps(response, indent=2)}")

            else:
                break

        #if store and response.get("entry"):
        #    for entry in response["entry"]:
        #        self.db.put(entry[

        return response

    @classmethod
    def _xml_to_json(cls, xml: str):
        def _tag(tag: str) -> str:
            return tag.split("}")[-1]

        def _to_json(elem: ET.Element):
            sub_elements = list(elem)
            if not sub_elements:
                data = {
                    "text": elem.text,
                }
                if elem.attrib:
                    data["attr"] = elem.attrib

                return data

            data = {}
            for e in elem:
                key = _tag(e.tag)
                if key in data:
                    if not isinstance(data[key], list):
                        data[key] = [data[key]]
                    data[key].append(_to_json(e))
                else:
                    data[key] = _to_json(e)

            return data

        return _to_json(ET.fromstring(xml))
