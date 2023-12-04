from pathlib import Path
import unittest

from src.teletext.teletext_ng import TeletextNg
from src.teletext import charset, coding


class TestTeletextNg(unittest.TestCase):

    def test_100(self):
        iterator = TeletextNg(Path("~/Downloads/2023-01-at.ProSieben.zip").expanduser())
        #iterator = TeletextNg(Path("~/Downloads/2023-01-de.KI.KA.zip").expanduser())
        for timestamp, page_index, page in iterator.iter_pages():
            if page_index == (100, 0):#(458, 0):
                print(timestamp, page_index)

                for i, c in enumerate(page):
                    #ch = coding.hamming8_decode(c)
                    #ch = f"{c:02x}"
                    #ch = chr(c & 0x7f)
                    #ch = "X" if c & (~0x7f) else " "
                    ch = chr(c & 0x7f) + " " if 32 <= c else f"{c:02x}"
                    #ch = charset.g0['default'].get(c & 0x7f, '?')
                    #ch = charset.g1.get(c & 0x7f, '?')
                    print(f"{ch} ", end="")
                    if (i + 1) % 42 == 0:
                        print()
                print()
                break