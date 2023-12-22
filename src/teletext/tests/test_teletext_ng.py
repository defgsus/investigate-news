from pathlib import Path
import unittest

from src.teletext.teletext_ng import TeletextNG
from src.teletext import charset, coding


class TestTeletextNg(unittest.TestCase):

    def test_100(self):
        #iterator = TeletextNG(Path("~/Downloads/2023-01-at.ProSieben.zip").expanduser())
        iterator = TeletextNG(Path("~/Downloads/2023-01-de.KI.KA.zip").expanduser())
        for page in iterator.iter_pages():
            print("XX", page.page, page.sub_page)
            print(page.to_ansi_colored())

            input(">")