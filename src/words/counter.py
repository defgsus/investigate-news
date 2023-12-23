import json
from pathlib import Path
from typing import Hashable, Dict, Tuple, Iterable, Union, Optional, Callable, List, Sequence

from ..mixin import StateDictMixin
from .calcdict import CalcDict


class TokenCounter(StateDictMixin):

    def __init__(
            self,
            tokens: Optional[Dict[Hashable, int]] = None,
            num_all: int = 0,
    ):
        self.tokens: CalcDict = CalcDict(tokens or dict())
        self.num_all = num_all

    def sorted_keys(self, key: Optional[Callable] = None, reverse: bool = True) -> List[Hashable]:
        return list(self.tokens.sorted(key=key, reverse=reverse))

    def sort(self, key: Optional[Callable] = None, reverse: bool = True):
        self.tokens = self.tokens.sorted(key=key, reverse=reverse)

    def freq_of(self, token: Hashable) -> float:
        return 0. if not self.num_all else self.tokens.get(token, 0.) / self.num_all

    def to_freq(self) -> CalcDict:
        return self.tokens / self.num_all

    def to_lowercase(self) -> "TokenCounter":
        return self.map_key(lambda k: k.lower())

    def map_key(self, func: Callable) -> "TokenCounter":
        tc = TokenCounter(num_all=self.num_all)
        for key, value in self.tokens.items():
            key = func(key)
            tc.tokens[key] = tc.tokens.get(key, 0) + value
        return tc

    def state_dict(self) -> dict:
        return {
            "num_all": self.num_all,
            "tokens": self.tokens,
        }

    @classmethod
    def from_state_dict(cls, data: dict):
        return TokenCounter(**data)

    def add(
            self,
            *token: Hashable,
            count: int = 1,
            count_all: int = 1,
    ):
        for tok in token:
            self.tokens[tok] = self.tokens.get(tok, 0) + count
        self.num_all += count_all * len(token)
        return self

    def idf(self) -> CalcDict:
        return self.tokens.inverse(self.num_all)

    def filter(
            self,
            tokens: Optional[Sequence[Hashable]] = None,
            inplace: bool = False,
    ) -> "TokenCounter":
        """TODO: num_all is not adjusted"""
        new_tokens = self.tokens

        if tokens is not None:
            new_tokens = new_tokens.filtered(keys=tokens)

        if inplace:
            self.tokens = new_tokens
            return self

        return TokenCounter(
            tokens=new_tokens,
            num_all=self.num_all,
        )

    def dump(self, count: int = 50, sort_key: Optional[Callable] = None, file=None):
        min_c = min(self.tokens.values())
        max_c = max(self.tokens.values())
        print(f"tokens / unique: {self.num_all:,} / {len(self.tokens):,}")
        print(f"count min / max: {min_c:,} / {max_c:,}")

        keys = self.sorted_keys(key=sort_key)[:count]
        max_len = max(len(str(key)) for key in keys)
        for key in keys:
            print(f"{str(key):{max_len}} {self.tokens[key]:9,} ({self.freq_of(key):.5f})", file=file)

