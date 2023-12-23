import json
from pathlib import Path
from typing import Hashable, Dict, Tuple, Iterable, Union, Optional, Callable, List, Sequence

from ..mixin import StateDictMixin
from .calcdict import CalcDict
from .counter import TokenCounter

class TokenCounterBuckets(StateDictMixin):

    def __init__(self):
        self.buckets: Dict[Hashable, TokenCounter] = {}

    def state_dict(self) -> dict:
        return {
            bucket_key: bucket.state_dict()
            for bucket_key, bucket in self.buckets.items()
        }

    @classmethod
    def from_state_dict(cls, data: dict):
        instance = cls()
        instance.buckets = {
            key: TokenCounter.from_state_dict(bucket)
            for key, bucket in data.items()
        }
        return instance

    def add(
            self,
            bucket_key: Hashable,
            tokens: Iterable[Hashable],
            count: int = 1,
            count_all: int = 1,
    ):
        if bucket_key not in self.buckets:
            self.buckets[bucket_key] = TokenCounter()

        self.buckets[bucket_key].add(*tokens, count=count, count_all=count_all)
        return self

    def dump(self, count: int = 10, sort_key: Optional[Callable] = None, file=None):
        for key, bucket in self.buckets.items():
            print(f"-- bucket {key} --")
            bucket.dump(count=count, sort_key=sort_key, file=file)
