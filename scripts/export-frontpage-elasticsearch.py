"""
Exports all articles from the

    github.com/defgsus/frontpage-archive-

repos to elasticsearch.

"""
import json
import datetime
from pathlib import Path

from elastipy import Exporter
import dateutil.parser
from tqdm import tqdm

from src.frontpage import FrontpageIterator


TEXT_TYPE = {
    "type": "text",
    "analyzer": "standard",
    "term_vector": "with_positions_offsets_payloads",
    "store": True,
    "fielddata": True,
}


class FrontpageExporter(Exporter):
    INDEX_NAME = "frontpage-archive"

    MAPPINGS = {
        "properties": {
            "timestamp": {"type": "date"},
            "timestamp_hour": {"type": "integer"},
            "timestamp_weekday": {"type": "keyword"},
            "timestamp_week": {"type": "keyword"},

            "commit_hash": {"type": "keyword"},

            "channel": {"type": "keyword"},
            "category": {"type": "keyword"},
            "author": {"type": "keyword"},
            "topic": {"type": "keyword"},
            "url": {"type": "keyword"},
            "title": TEXT_TYPE,
            "teaser": TEXT_TYPE,
            "image_title": TEXT_TYPE,
        }
    }

    def get_document_id(self, data) -> str:
        return f'{data["timestamp"]}-{data["channel"]}-{data["category"]}-{data["rank"]}'

    def transform_document(self, data: dict) -> dict:
        data = data.copy()
        self._add_timestamp(data, "timestamp")
        return data

    @classmethod
    def _add_timestamp(cls, data: dict, key: str):
        if not isinstance(data[key], datetime.datetime):
            data[key] = dateutil.parser.parse(data[key])
        data[f"{key}_hour"] = data[key].hour
        data[f"{key}_weekday"] = data[key].strftime("%w %A")
        data[f"{key}_week"] = "%s-%s" % data[key].isocalendar()[:2]


def export_elasticsearch_daily_buckets():
    exporter = FrontpageExporter(index_postfix="daily")
    # exporter.delete_index()
    exporter.update_index()

    def _yield_items():
        since_date = None
        result = exporter.search().sort("-timestamp").execute()
        if result.documents:
            since_date = result.documents[0]["timestamp"][:10]

        if since_date:
            print("skipping existing data before", since_date)

        frontpages = FrontpageIterator(
            since_date=since_date,
        )
        for fp, a in frontpages.iter_articles_first_of_bucket(
                bucket_key=lambda fp, a: f"{fp.timestamp[:10]}-{fp.channel}-{fp.category}-{a.url}"
        ):
            yield {
                "timestamp": fp.timestamp_dt,
                "commit_hash": fp.commit_hash,
                "channel": fp.channel,
                "category": fp.category,
                "rank": a.rank,
                "topic": a.topic,
                "url": a.url,
                "title": a.title,
                "teaser": a.teaser,
                "image_title": a.image_title,
                "image_url": a.image_url,
                "author": a.author,
            }

    #counts = {}
    #for i, item in enumerate(_yield_items()):
    #    key = f'{item["channel"]}'#-{item["category"]}'
    #    counts[key] = counts.get(key, 0) + 1
    #    if i % 100 == 0:
    #        print(json.dumps(counts, indent=2))

    exporter.export_list(_yield_items(), chunk_size=1000)


def test():
    frontpages = FrontpageIterator(
        since_date="2022-11-01",
        until_date="2023-01-10",
    )
    counts = {}
    for i, (fp, article) in enumerate(frontpages.iter_articles_first_of_bucket(
            bucket_key=lambda fp, a: f"{fp.timestamp[:10]}-{fp.channel}-{fp.category}-{a.url}"
    )):
        key = f'{fp.channel}'#-{fp.category}'
        counts[key] = counts.get(key, 0) + 1
        if i % 10000 == 0:
            print(json.dumps(counts, indent=2))



if __name__ == "__main__":
    #test()
    export_elasticsearch_daily_buckets()
