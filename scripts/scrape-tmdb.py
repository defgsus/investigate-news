import datetime
import json
import time
import gzip

from tqdm import tqdm

from src.tmdb import TMDB



def scrape():
    db = TMDB()

    #movie = db.get_movie(11)
    #print(json.dumps(movie, indent=2))

    for movie in tqdm(db.iter_movie_ids(datetime.date(2023, 12, 1), adult=False)):
        movie = db.get_movie(movie["id"])
        time.sleep(1. / 30.)
        #print(json.dumps(movie, indent=2))
        #break


def export():
    db = TMDB()

    with gzip.open("tmdb.ndjson.gz", "wt") as fp:

        for movie in tqdm(db.iter_movie_ids(datetime.date(2023, 12, 1), adult=False)):
            movie = db.get_movie(movie["id"])
            fp.write(json.dumps(movie) + "\n")



if __name__ == "__main__":
    export()
