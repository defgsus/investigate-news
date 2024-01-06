import datetime
import json
import time
import itertools

from tqdm import tqdm

from src.tmdb import TMDB
from src.ndjson import NDJson


def iter_movies(
        until_date: datetime.date = datetime.date(2023, 12, 1),
):
    db = TMDB()
    for movie in tqdm(itertools.chain(
            db.iter_movie_ids(until_date, adult=True),
            db.iter_movie_ids(until_date, adult=False)
    )):
        try:
            movie = db.get_movie(movie["id"])
        except RuntimeError as e:
            print(e)

        yield movie


def scrape():
    for m in iter_movies():
        time.sleep(1. / 30.)
        #print(json.dumps(movie, indent=2))
        #break


def export():
    with NDJson("tmdb.ndjson.gz", "w") as fp:
        for m in iter_movies():
            fp.write(m)




if __name__ == "__main__":
    scrape()
    #export()
