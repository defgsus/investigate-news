import datetime
import json
import time

from tqdm import tqdm

from src.tmdb import TMDB



def main():
    db = TMDB()

    #movie = db.get_movie(11)
    #print(json.dumps(movie, indent=2))

    for movie in tqdm(db.iter_movie_ids(datetime.date(2023, 12, 1), adult=False)):
        db.get_movie(movie["id"])
        time.sleep(1. / 30.)
        # print(json.dumps(movie, indent=2))





if __name__ == "__main__":
    main()
