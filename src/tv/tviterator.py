import datetime
import json
import tarfile
import dataclasses
from pathlib import Path
from typing import Optional, Tuple, List, Iterable, Generator, Union, Callable

import dateutil.parser
from tqdm import tqdm


@dataclasses.dataclass
class TvProgram:
    id: str
    url: str
    channel: str
    title: str
    date: str
    length: int  # minutes
    sub_title: Optional[str] = None
    genre: Optional[str] = None
    description: Optional[str] = None
    season: Optional[int] = None
    episode: Optional[int] = None
    year: Optional[int] = None
    countries: Optional[List[str]] = None

    @property
    def date_dt(self) -> datetime.datetime:
        if not hasattr(self, "_date_dt"):
            self._date_dt = dateutil.parser.parse(self.date)
        return self._date_dt


class TvIterator:

    PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent.parent
    SNAPSHOT_PATH = "docs/data"

    def __init__(
            self,
            since_date: Optional[Union[str, datetime.date, datetime.datetime]] = None,
            before_date: Optional[Union[str, datetime.date, datetime.datetime]] = None,
            verbose: bool = True,
    ):
        self.since_date = None if since_date is None else str(since_date)
        self.before_date = None if before_date is None else str(before_date)
        self.verbose = verbose
        self.repos: List[Path] = []

        for repo_name in (
                "tv-archive",
        ):
            path = self.PROJECT_ROOT.parent / repo_name
            if path.exists():
                self.repos.append(path)
            else:
                print(f"NOT FOUND:", path)

    def iter_program(self, raw: bool = False) -> Generator[TvProgram, None, None]:
        for repo_path in self.repos:
            for filename in sorted((repo_path / self.SNAPSHOT_PATH).rglob("*.ndjson")):
                with filename.open() as fp:
                    for line in fp.readlines():
                        data = json.loads(line)
                        if self.since_date is None or data["date"] >= self.since_date:
                            if self.before_date is None or data["date"] < self.before_date:
                                yield data if raw else TvProgram(**data)


if __name__ == "__main__":
    tv = TvIterator()
    for prog in tv.iter_program():
        print(prog)