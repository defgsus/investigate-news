import zipfile
import re
from pathlib import Path
from io import BytesIO
from typing import Union, Generator

from .t42 import T42Page


class TeletextNG:

    _re_timestamp = re.compile(r".*(\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d).*")
    _re_page = re.compile(r".*(\d\d\d)-(\d+).t42")

    COLORS = "brgylmcw"

    def __init__(self, filename: Union[str, Path]):
        self.filename = filename

    def iter_pages(self) -> Generator[T42Page, None, None]:
        # open outer zip
        with zipfile.ZipFile(self.filename) as main_zip:
            # for each zipped zip
            for zip_filename in main_zip.filelist:
                match = self._re_timestamp.match(zip_filename.filename)
                if match:
                    timestamp = match.groups()[0]
                    data = BytesIO(main_zip.read(zip_filename))
                    data.seek(0)
                    # open zipped zip
                    with zipfile.ZipFile(data) as zip_file:
                        for filename in zip_file.filelist:
                            match = self._re_page.match(filename.filename)
                            if match:
                                page, sub_page = [int(g) for g in match.groups()]
                                yield T42Page(zip_file.read(filename), timestamp=timestamp)
