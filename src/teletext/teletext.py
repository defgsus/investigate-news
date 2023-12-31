import json
from pathlib import Path
from typing import List, Optional, TextIO, Tuple, Union, IO, Dict

from .page import TeletextPage
from .categories import get_page_category


class Teletext:
    
    def __init__(self):
        self.pages: Dict[Tuple[int, int], TeletextPage] = {}
        self.page_index: List[Tuple[int, int]] = []
        self.timestamp: str = None
        self.channel: str = None
        self.commit_hash: str = None

    def __repr__(self):
        return f"{self.__class__.__name__}({self.timestamp}, {self.channel}, {len(self.pages)})"

    @classmethod
    def from_ndjson(
            cls,
            file: Union[str, Path, IO, List[str], bytes],
    ) -> "Teletext":
        if isinstance(file, (str, Path)):
            lines = Path(file).read_text().strip().splitlines()
        elif isinstance(file, list):
            lines = file
        elif isinstance(file, bytes):
            lines = file.replace(b"\x96\xc2\x00\x0a", b"").decode().splitlines()
        else:
            content = file.read()
            if isinstance(content, bytes):
                content = content.decode()
            lines = content.strip().splitlines()

        tt = cls()

        cur_page = None
        for line_idx, line in enumerate(lines):

            if line.startswith("{"):
                line = json.loads(line)

                # file header
                if "scraper" in line:
                    tt.timestamp = line["timestamp"]
                    tt.channel = line["scraper"]
                    continue

                # page header
                cur_page = TeletextPage()
                cur_page.channel = tt.channel
                cur_page.index = line["page"]
                cur_page.sub_index = line["sub_page"]
                cur_page.timestamp = line["timestamp"]
                cur_page.error = line.get("error")
                cur_page.category = get_page_category(tt.channel, cur_page.index, cur_page.timestamp)

                index = (cur_page.index, cur_page.sub_index)
                tt.pages[index] = cur_page
                tt.page_index.append(index)

            # page content
            else:
                assert cur_page, f"line before page"
                cur_page._lines_ndjson.append(line)

        tt.page_index.sort()
        return tt

    def get_page(self, page: int, sub_page: Optional[int] = None) -> Optional[TeletextPage]:
        if sub_page is not None:
            return self.pages.get((page, sub_page))
        for key in self.page_index:
            if key[0] == page:
                return self.pages[key]

    def get_next_page(self, page: int, sub_page: int, dir: int = 1) -> Tuple[int, int]:
        page = page, sub_page
        if dir == 0:
            for p in self.page_index:
                if p >= page:
                    return p
            return self.page_index[0]

        if dir > 0:
            for p in self.page_index:
                if p > page:
                    return p
            return self.page_index[0]

        elif dir < 0:
            for p in reversed(self.page_index):
                if p < page:
                    return p
            return self.page_index[-1]

        return page
