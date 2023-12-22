"""
T42 format parser
based on https://github.com/Casandro/teletext_ng/blob/main/tools/dump_tta_text_colour.c
"""
from typing import Optional, List, Tuple

from src import console


class T42Page:

    def __init__(self, data: bytes, timestamp: Optional[str] = None, channel: Optional[str] = None):
        self.data = data
        self.timestamp = timestamp
        self.channel = channel
        self.language = 0
        self._page = 0
        self._sub_page = 0
        self._color_f = 7
        self._color_b = 0
        self._mosaik = 0
        self._blocks = None
        self._XX = 0

    @property
    def blocks(self) -> List[List[Tuple[str, int, int, int]]]:
        if self._blocks is None:
            self._blocks = self._parse()

        return self._blocks

    @property
    def page(self) -> int:
        if not self._page:
            self._parse()
        return self._page

    @property
    def sub_page(self) -> int:
        if not self._page:
            self._parse()
        return self._sub_page

    def to_ansi_colored(self, header: bool = True, extra: bool = False) -> str:
        ansi = []
        color = (7, 0)
        for line_idx, line in enumerate(self.blocks):
            if line_idx == 0 and not header:
                continue
            if line_idx > 23 and not extra:
                break

            for b in line:
                if b[1:3] != color:
                    color = b[1:3]
                    ansi.append(console.ConsoleColors.escape(fore=color[0], back=color[1]))

                ansi.append(b[0])

            color = (7, 0)
            ansi.append(console.ConsoleColors.escape())

            ansi.append("\n")

        return "".join(ansi[:-1])

    def _parse(self):
        blocks = []
        for packet_idx in range(len(self.data) // 42):
            packet = self.data[packet_idx * 42: (packet_idx + 1) * 42]

            mpag = de_hamm(packet[1]) << 4 | de_hamm(packet[0])
            magazine = mpag & 0x7
            row = mpag >> 3
            start = 2

            if row == 0:
                page = de_hamm(packet[3]) << 4 | de_hamm(packet[2])
                sub = de_hamm(packet[4]) | (de_hamm(packet[5]) << 4) | (de_hamm(packet[6]) << 8) | (de_hamm(packet[7]) << 12)
                contr = de_hamm(packet[9]) << 4 | de_hamm(packet[8])

                fullpage = magazine << 8 | page
                if fullpage < 0x100:
                    fullpage = fullpage | 0x800

                self._page = int(f"{fullpage:03x}")
                self._sub_page = (sub & 0x3f7f) or 1
                self.language = (contr >> 4) & 0x7
                start = 10

            blocks.append(self._parse_line(packet, start))

        return blocks

    def _parse_line(self, line: bytes, start: int):
        self._color_f = 7
        self._color_b = 0
        self._mosaik = 0

        blocks = []
        for i in range(start - 2):
            blocks.append((" ", self._color_f, self._color_b, self._mosaik))
        line = line[start:]

        for c in line:
            c = c & 0x7f

            if c < 0x20:
                if c & 0xf <= 0x07:
                    self._color_f = c & 0x07
                if c <= 0x07:
                    self._mosaik = 0
                if 0x10 <= c <= 0x17:
                    self._mosaik = 1
                if c == 0x1c:
                    self._color_b = 0
                if c == 0x1d:
                    self._color_b = self._color_f

                blocks.append((" ", self._color_f, self._color_b, self._mosaik))

            if c >= 0x20:
                glyph = c - 0x20

                if self._mosaik == 0:
                    if c == 0x23: glyph = 0xA0 + self.language * 0x10
                    if c == 0x24: glyph = 0xA1 + self.language * 0x10
                    if c == 0x40: glyph = 0xA2 + self.language * 0x10
                    if c == 0x5B: glyph = 0xA3 + self.language * 0x10
                    if c == 0x5C: glyph = 0xA4 + self.language * 0x10
                    if c == 0x5D: glyph = 0xA5 + self.language * 0x10
                    if c == 0x5E: glyph = 0xA6 + self.language * 0x10
                    if c == 0x5F: glyph = 0xA7 + self.language * 0x10
                    if c == 0x60: glyph = 0xA8 + self.language * 0x10
                    if c == 0x7B: glyph = 0xA9 + self.language * 0x10
                    if c == 0x7C: glyph = 0xAa + self.language * 0x10
                    if c == 0x7D: glyph = 0xAb + self.language * 0x10
                    if c == 0x7E: glyph = 0xAc + self.language * 0x10

                else:
                    if 0x20 <= c <= 0x3f: glyph = c - 0x20 + 0x60
                    if 0x40 <= c <= 0x5f: glyph = c - 0x20
                    if 0x60 <= c <= 0x7f: glyph = c - 0x60 + 0x80

                char = glyph_to_utf8(glyph)
                blocks.append((char or " ", self._color_f, self._color_b, self._mosaik))

        while len(blocks) < 40:
            blocks.append((" ", self._color_f, self._color_b, self._mosaik))

        return blocks


VD_GLYPH_TO_UTF8 = [
#   x0   x1   x2   x3   x4   x5   x6   x7   x8   x9   xA   xB   xC   xD   xE   xF
    " ", "!","\"", "#", "¤", "%", "&", "'", "(", ")", "*", "+", ",", "-", ".", "/",  # 0x
    "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", ":", ";", "<", "=", ">", "?",  # 1x
    "@", "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O",  # 2x
    "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z", "[","\\", "]", "^", "_",  # 3x
    "`", "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o",  # 4x
    "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z", "{", "|", "}", "~", "■",  # 5x
    " ", "🬀", "🬁", "🬂", "🬃", "🬄", "🬅", "🬆", "🬇", "🬈", "🬉", "🬊", "🬋", "🬌", "🬍", "🬎",  # 6x
    "🬏", "🬐", "🬑", "🬒", "🬓", "▌", "🬔", "🬕", "🬖", "🬗", "🬘", "🬙", "🬚", "🬛", "🬜", "🬝",  # 7x
    "🬞", "🬟", "🬠", "🬡", "🬢", "🬣", "🬤", "🬥", "🬦", "🬧", "▐", "🬨", "🬩", "🬪", "🬫", "🬬",  # 8x
    "🬭", "🬮", "🬯", "🬰", "🬱", "🬲", "🬳", "🬴", "🬵", "🬶", "🬷", "🬸", "🬹", "🬺", "🬻", "█",  # 9x
    "#", "¤", "@", "[", "\\","]", "^", "_", "{", "|", "}", "~", " ", " ", " ", " ",  # Ax English
    "#", "$", "§", "Ä", "Ö", "Ü", "^", "_", "°", "ä", "ö", "ü", "ß", " ", " ", " ",  # Bx German
    "#", "¤", "É", "Ä", "Ö", "Å", "Ü", "_", "é", "ä", "ö", "å", "ü", " ", " ", " ",  # Cx Swedish/Finnish/Hungarian
    "£", "$", "é", "°", "ç", "→", "↑", "#", "ù", "à", "ò", "è", "ì", " ", " ", " ",  # Dx Italian
    "é", "ï", "à", "ë", "ê", "ù", "î", "#", "è", "â", "ô", "û", "ç", " ", " ", " ",  # Ex French
    "ç", "$", "¡", "á", "é", "í", "ó", "ú", "¿", "ü", "ñ", "è", "à", " ", " ", " ",  # Fx Portuguese/Spanish
]
assert len(VD_GLYPH_TO_UTF8) == 0x100, len(VD_GLYPH_TO_UTF8)

def glyph_to_utf8(glyph: int) -> Optional[str]:
    if 0 <= glyph <= 0xff:
        return VD_GLYPH_TO_UTF8[glyph]

def bit(x: int, which: int):
    return (x >> which) & 1


def de_hamm(x: int):
    return bit(x, 1) | (bit(x, 3) << 1) | (bit(x, 5) << 2) | (bit(x, 7) << 3)
