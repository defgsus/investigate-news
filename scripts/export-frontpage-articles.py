"""
Exports all unique articles from the

    github.com/defgsus/frontpage-archive*

repos to a json file.

"""
import json
import time
import datetime
from pathlib import Path

import dateutil.parser
from tqdm import tqdm

from src.frontpage import FrontpageIterator


def export_unique_frontpage_articles(
        filename: str = "./data/frontpage-articles.json",
):
    frontpages = FrontpageIterator()
    article_map = {}

    last_print_time = time.time()
    try:
        for fp, a in frontpages.iter_articles():
            key = a.url
            if key not in article_map:
                article_map[key] = {
                    "count": 1,
                    "timestamp_min": fp.timestamp,
                    "timestamp_max": fp.timestamp,
                    "commit_hash": fp.commit_hash,
                    "channel": fp.channel,
                    "category": fp.category,
                    "rank_min": a.rank,
                    "rank_max": a.rank,
                    "topic": a.topic,
                    "url": a.url,
                    "title": a.title,
                    "teaser": a.teaser,
                    "image_title": a.image_title,
                    "image_url": a.image_url,
                    "author": a.author,
                }
            else:
                am = article_map[key]
                am["count"] += 1
                am["timestamp_min"] = min(am["timestamp_min"], fp.timestamp)
                am["timestamp_max"] = max(am["timestamp_max"], fp.timestamp)
                am["rank_min"] = min(am["rank_min"], a.rank)
                am["rank_max"] = max(am["rank_max"], a.rank)

            cur_time = time.time()
            if cur_time - last_print_time > 30:
                last_print_time = cur_time
                print(len(article_map), "articles")

        print(len(article_map), "articles")

    except KeyboardInterrupt:
        if Path(filename).exists():
            print(len(article_map), "articles")
            while True:
                a = input(f"Overwrite {filename} (Y/n)? ")
                if a in ("", "y", "Y"):
                    break
                if a in "nN":
                    return

    print("writing", filename)
    with open(filename, "wt") as fp:
        for value in article_map.values():
            fp.write(json.dumps(value) + "\n")

    #Path().write_text(json.dumps(article_map))
    # print(json.dumps(article_map, indent=2))


if __name__ == "__main__":
    export_unique_frontpage_articles()
