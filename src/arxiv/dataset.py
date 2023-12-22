import zipfile
import json
from pathlib import Path
from io import TextIOWrapper
from typing import Optional

from tqdm import tqdm
import dateutil


def iter_arxiv_papers(
        category: Optional[str] = None,
        update_date: Optional[str] = None,
):
    with zipfile.ZipFile(Path("~/prog/data/arxiv-metadata-oai-snapshot.json.zip").expanduser()) as zf:
        with zf.open("arxiv-metadata-oai-snapshot.json") as fp:
            with TextIOWrapper(fp) as tfp:
                for line in tqdm(tfp, total=2_385_180):
                    if category and category not in line:
                        continue
                    if update_date and update_date not in line:
                        continue

                    paper = json.loads(line)
                    if category and category not in paper["categories"]:
                        continue
                    if update_date and update_date not in paper["update_date"]:
                        continue

                    yield paper
