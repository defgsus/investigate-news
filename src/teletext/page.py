import datetime
import io
import json
from typing import List, Optional, TextIO, Tuple, Union

from ..console import ConsoleColors as CC
from ..words import tokenize, concat_split_words


class TeletextPage:
    """
    Single page representation.

    This class wraps between the encoded ndjson file format
    and the ANSI representation.
    """
    
    COLOR_CONSOLE_MAPPING = {
        "b": CC.BLACK,
        "r": CC.RED,
        "g": CC.GREEN,
        "y": CC.YELLOW,
        "l": CC.BLUE,
        "m": CC.PURPLE,
        "c": CC.CYAN,
        "w": CC.WHITE,
    }

    BOOL_RGB_TO_TELETEXT_MAPPING = {
        (False, False, False): "b",
        (True, False, False): "r",
        (False, True, False): "g",
        (True, True, False): "y",
        (False, False, True): "l",
        (True, False, True): "m",
        (False, True, True): "c",
        (True, True, True): "w",
    }

    class Block:
        """
        Representation of a text block with its attributes like
         - foreground and background color
         - the extended character set
         - a teletext page link
        """
        def __init__(
                self,
                text: str,
                color: Optional[str] = None,
                bg_color: Optional[str] = None,
                char_set: int = 0,
                link: Optional[Union[int, Tuple[int, int], List[int]]] = None,
        ):
            assert color is None or color in TeletextPage.COLOR_CONSOLE_MAPPING, color
            assert bg_color is None or bg_color in TeletextPage.COLOR_CONSOLE_MAPPING, bg_color
            self.text = text
            self.color = color
            self.bg_color = bg_color
            self.char_set = char_set
            self._link = None
            self.link = link

        def __eq__(self, other) -> bool:
            if not isinstance(other, self.__class__):
                return False
            return self.text == other.text and not self.has_different_attribute(other)

        @property
        def link(self) -> Union[int, List[int]]:
            return self._link

        @link.setter
        def link(self, link: Optional[Union[int, Tuple[int, int], List[int]]]):
            if isinstance(link, (tuple, list)):
                self._link = [int(l) for l in link]
                if len(self._link) == 1:
                    self._link = self._link[0]
                elif len(self._link) != 2:
                    raise ValueError(f"Invalid block link {link}")
            else:
                self._link = int(link) if link is not None else None

        def has_different_attribute(self, other: "TeletextPage.Block") -> bool:
            return self.color != other.color \
                or self.bg_color != other.bg_color \
                or self.char_set != other.char_set \
                or self.link != other.link

        def splitlines(self) -> List["TeletextPage.Block"]:
            if "\n" not in self.text:
                return [self]
            return [
                self.__class__(line, self.color, self.bg_color, self.char_set)
                for line in self.text.splitlines()
            ]

        def to_json(self) -> list:
            color = "".join((self.color or "_", self.bg_color or "_"))
            if self.char_set:
                color += str(self.char_set)
            attrs = [color]

            if self.link:
                if isinstance(self.link, (list, tuple)):
                    attrs.append(list(self.link))
                else:
                    attrs.append(self.link)

            return attrs + [self.text]

        def to_ansi(self, colors: bool = True) -> str:
            block_str = self.text

            if colors:
                block_str = CC.escape(
                    TeletextPage.COLOR_CONSOLE_MAPPING[self.color or "w"],
                    TeletextPage.COLOR_CONSOLE_MAPPING[self.bg_color or "b"]
                ) + block_str + CC.escape()

            return block_str

        @classmethod
        def from_json(cls, block: List) -> "TeletextPage.Block":
            kwargs = {
                "text": block[-1],
                "color": block[0][0] if block[0][0] != "_" else None,
                "bg_color": block[0][1] if block[0][1] != "_" else None,

            }
            if len(block[0]) > 2:
                kwargs["char_set"] = int(block[0][2])

            if len(block) > 2:
                kwargs["link"] = block[1]

            return cls(**kwargs)

    def __init__(self):
        self._lines: List[List[TeletextPage.Block]] = None
        self._lines_ndjson: List[str] = []
        self.index = 100
        self.sub_index = 1
        self.channel: str = None
        self.timestamp: str = None
        self.error: str = None
        self.category: str = None

    def __str__(self):
        return f"{self.index}/{self.sub_index}({len(self._lines_ndjson)} lines)"

    def __eq__(self, other) -> bool:
        """
        Only compares the content, not the timestamp or index!
        """
        if not isinstance(other, TeletextPage):
            return False
        return self._lines_ndjson == other._lines_ndjson

    @property
    def timestamp_dt(self) -> datetime.datetime:
        return datetime.datetime.strptime(self.timestamp, "%Y-%m-%dT%H:%M:%S")

    @property
    def lines(self):
        if self._lines is None:
            self._lines = []

            for line_idx, line in enumerate(self._lines_ndjson):
                try:
                    line = json.loads(line)
                except:
                    print(f"ERROR in line #{line_idx} '{line}'")
                    continue

                self._lines.append([
                    TeletextPage.Block.from_json(block)
                    for block in line
                ])
        return self._lines

    def to_ndjson(self, file: Optional[TextIO] = None) -> Optional[str]:
        if file is None:
            file = io.StringIO()
            self.to_ndjson(file)
            file.seek(0)
            return file.read()

        header = {
            "page": self.index,
            "sub_page": self.sub_index,
            "timestamp": self.timestamp,
        }
        if self.error:
            header["error"] = self.error

        print(json.dumps(header, ensure_ascii=False, separators=(',', ':')), file=file)

        if not self.error:
            for line in self.lines:
                json_line = [b.to_json() for b in line]
                print(json.dumps(json_line, ensure_ascii=False, separators=(',', ':')), file=file)

    def to_ansi(self, file: Optional[TextIO] = None, colors: bool = True, border: bool = False) -> Optional[str]:
        if file is None:
            file = io.StringIO()
            self.to_ansi(file, colors=colors, border=border)
            file.seek(0)
            return file.read()

        if not border:
            for line in self.lines:
                for block in line:
                    block_str = block.to_ansi(colors=colors)
                    print(block_str, end="", file=file)

                print(file=file)

        else:
            lines = [
                "".join(block.to_ansi(colors=False) for block in line)
                for line in self.lines
            ]
            width = max(0, 0, *(len(l) for l in lines))

            if colors:
                color_lines = [
                    "".join(block.to_ansi(colors=True) for block in line)
                    for line in self.lines
                ]

                c = CC.escape(CC.WHITE, bright=False)
                off = CC.escape()
                print(c + "▛" + "▀" * width + "▜" + off, file=file)
                for c_line, line in zip(color_lines, lines):
                    print(c + "▌" + off + c_line + " " * (width - len(line)) + c + "▐" + off, file=file)
                print(c + "▙" + "▄" * width + "▟" + off, file=file)
            else:
                print("▛" + "▀" * width + "▜", file=file)
                for line in lines:
                    print("▌" + line + " " * (width - len(line)) + "▐", file=file)
                print("▙" + "▄" * width + "▟", file=file)

    def to_image(self):
        from .image_renderer import TeletextImageRenderer
        renderer = TeletextImageRenderer()
        return renderer.render(self)

    def to_text(self, concat_split_words: bool = True) -> str:
        """
        Returns everything that is not graphics or numbers.

        Also concats bro-
        ken lines together.
        """
        texts = []
        for line in self.lines:
            for block in line:
                for c in block.text:
                    if ord(c) < 0x1bf00 and not 0x2500 <= ord(c) < 0x2600 and not "0" <= c <= "9":
                        texts.append(c)

            texts.append("\n")
        text = "".join(texts)

        if concat_split_words:
            text = globals()["concat_split_words"](text)
        return text

    def to_tokens(self, lowercase: bool = False, concat_split_words: bool = True) -> List[str]:
        text = self.to_text(concat_split_words=concat_split_words)
        return tokenize(text, lowercase=lowercase)

    def _simplify_line(self, line: List[Block]) -> List[Block]:
        """
        Merge blocks of equal attributes together

        Returns new list but the Block instances may have changed!
        """
        simple_line = []
        prev_block = None
        for block in line:
            if not prev_block:
                prev_block = block
            elif block.has_different_attribute(prev_block):
                simple_line.append(prev_block)
                prev_block = block
            else:
                prev_block.text += block.text

        if prev_block:
            simple_line.append(prev_block)

        return simple_line

