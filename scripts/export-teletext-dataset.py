"""
Export unqiue teletext pages
"""
import json
import time
import datetime
import hashlib
import gzip
from pathlib import Path

import dateutil.parser
from tqdm import tqdm

from src.teletext import TeletextIterator


def export_unique_teletext_pages(
        filename: str = "./data/teletext.ndjson.gz",
):
    tt_iterator = TeletextIterator()
    page_hashes = set()

    num_duplicates = 0
    num_pages = 0
    last_print_time = time.time()

    with gzip.open(filename, "wt") as fp:
        try:
            for tt in tt_iterator.iter_teletexts():
                for page in tt.pages.values():
                    text = page.to_ansi(colors=False)

                    # cut status line
                    if tt.channel in ("zdf", "zdf-info", "zdf-neo", "ntv", "sr"):
                        text = text[text.find("\n") + 1:]

                    page_hash = hashlib.md5(text.encode()).hexdigest()
                    if page_hash in page_hashes:
                        num_duplicates += 1

                    num_pages += 1
                    page_hashes.add(page_hash)

                    fp.write(json.dumps({
                        "channel": tt.channel,
                        "timestamp": tt.timestamp,
                        "index": page.index,
                        "sub_index": page.sub_index,
                        "text": text,
                    }, ensure_ascii=False) + "\n")
                    #print(tt.channel, tt.timestamp, page.index, page.sub_index)
                    #print(text)

                cur_time = time.time()
                if cur_time - last_print_time >= 10:
                    last_print_time = cur_time
                    print(f"\npages: {num_pages:,} exported, {num_duplicates:,} duplicates")

        except KeyboardInterrupt:
            pass


def test_read(
        filename: str = "./data/teletext.ndjson.gz",
):
    with gzip.open(filename) as fp:
        for line in tqdm(fp):
            line = json.loads(line)
            print(line)



if __name__ == "__main__":
    export_unique_teletext_pages()
    #test_read()
