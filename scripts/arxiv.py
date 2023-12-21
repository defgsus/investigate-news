import argparse


from src.arxiv import Arxiv


def parse_args() -> dict:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "query", type=str,
        help="The arxiv search query",
    )
    parser.add_argument(
        "--cache-dir", type=str, default="arxiv-web-cache",
        help="Directory for the web request cache (using leveldb)",
    )

    return vars(parser.parse_args())


def main(
        query: str,
        cache_dir: str,
):
    arxiv = Arxiv(
        cache=cache_dir,
    )
    try:
        arxiv.query(query=query)

    finally:
        arxiv.cache.close()



if __name__ == "__main__":
    main(**parse_args())
