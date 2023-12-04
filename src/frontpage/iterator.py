import datetime
import json
import tarfile
import dataclasses
from pathlib import Path
from typing import Optional, Tuple, List, Iterable, Generator, Union, Callable

import dateutil.parser
from tqdm import tqdm


from giterator import Giterator


@dataclasses.dataclass
class FrontpageArticle:
    rank: int
    title: Optional[str] = None
    url: Optional[str] = None
    author: Optional[str] = None
    teaser: Optional[str] = None
    image_url: Optional[str] = None
    image_title: Optional[str] = None
    topic: Optional[str] = None


@dataclasses.dataclass
class Frontpage:
    channel: str
    category: str
    timestamp: str
    url: str
    scripts: List[dict]
    articles: List[FrontpageArticle]
    commit_hash: str

    @property
    def timestamp_dt(self) -> datetime.datetime:
        if not hasattr(self, "_timestamp_dt"):
            self._timestamp_dt = dateutil.parser.parse(self.timestamp)
        return self._timestamp_dt


class FrontpageIterator:
    """
    Access to 'frontpage-archive' throughout the git history
    """

    PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent.parent
    SNAPSHOT_PATH = "docs/snapshots"

    def __init__(
            self,
            channels: Optional[Iterable[str]] = None,
            categories: Optional[Iterable[str]] = None,
            since_date: Optional[Union[str, datetime.date, datetime.datetime]] = None,
            until_date: Optional[Union[str, datetime.date, datetime.datetime]] = None,
            verbose: bool = True,
    ):
        self.channels: List[str] = [] if channels is None else list(channels)
        self.categories: List[str] = [] if categories is None else list(categories)
        self.since_date = None if since_date is None else str(since_date)
        self.until_date = None if until_date is None else str(until_date)
        self.verbose = verbose
        self.repos = []

        for repo_name in (
                "frontpage-archive-2",
                "frontpage-archive-2023",
        ):
            path = self.PROJECT_ROOT.parent / repo_name
            if path.exists():
                self.repos.append(Giterator(path))
            else:
                print(f"NOT FOUND:", path)

    def iter_frontpages(self) -> Generator[Frontpage, None, None]:

        for repo in self.repos:
            commit_iterable = repo.iter_commits(
                self.SNAPSHOT_PATH,
                since=self.since_date,
                until=self.until_date,
            )
            if self.verbose:
                commit_iterable = tqdm(
                    commit_iterable,
                    desc=Path(repo.path).name,
                    total=repo.num_commits(self.SNAPSHOT_PATH),
                )

            for commit in commit_iterable:

                for file in commit.iter_files(self.SNAPSHOT_PATH):
                    filename_split = file.name.split("/")
                    name = filename_split[-1]
                    if not name.endswith(".json") or name.startswith("_"):
                        continue

                    channel = filename_split[-2]
                    category = name[:-5]

                    if self.channels and channel not in self.channels:
                        continue
                    if self.categories and category not in self.categories:
                        continue

                    data = json.loads(file.text())

                    yield Frontpage(
                        channel=channel,
                        category=category,
                        timestamp=data["timestamp"],
                        url=data["url"],
                        scripts=data["scripts"],
                        articles=[
                            FrontpageArticle(**a, rank=i)
                            for i, a in enumerate(data["articles"])
                        ],
                        commit_hash=commit.hash,
                    )

    def iter_articles(self) -> Generator[Tuple[Frontpage, FrontpageArticle], None, None]:
        for fp in self.iter_frontpages():
            for article in fp.articles:
                yield fp, article

    def iter_articles_first_of_bucket(
            self,
            bucket_key: Callable[[Frontpage, FrontpageArticle], str],
            max_buckets: int = 10_000,
            tolerance: int = 10_000,
    ) -> Generator[Tuple[Frontpage, FrontpageArticle], None, None]:
        buckets = {}
        for time, (fp, article) in enumerate(self.iter_articles()):
            key = bucket_key(fp, article)
            if key not in buckets:
                buckets[key] = (time, (fp, article))

            if len(buckets) > max_buckets + tolerance:
                sorted_keys = sorted(buckets.keys(), key=lambda k: buckets[k][0])
                i = 0
                while len(buckets) > max_buckets and i < len(sorted_keys):
                    yield buckets.pop(sorted_keys[i])[1]
                    i += 1

        sorted_keys = sorted(buckets.keys(), key=lambda k: buckets[k][0])
        for key in sorted_keys:
            yield buckets[key][1]

    def iter_frontpage_buckets(
            self,
            after_hash: Optional[str] = None,
    ):
        page_map = {}
        for page in self.iter_frontpages(after_hash=after_hash):
            key = f"{page.channel}-{page.category}"
            if key not in page_map:
                page_map[key] = page
            else:
                if page.timestamp[:10] != page_map[key].timestamp[:10]:
                    yield page_map[key]
                    page_map[key] = page
                else:
                    for new_article in page.articles:
                        exists = False
                        for article in page_map[key].articles:
                            if new_article.url == article.url:
                                exists = True
                                break
                        if not exists:
                            page_map[key].articles.append(new_article)

                    for new_script in page.scripts:
                        exists = False
                        for script in page_map[key].scripts:
                            if new_script.get("src") == script.get("src"):
                                exists = True
                                break
                        if not exists:
                            page_map[key].scripts.append(new_script)

        for page in page_map.values():
            yield page
