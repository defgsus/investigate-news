import pickle
import json
from pathlib import Path
from typing import Union, Tuple, BinaryIO, TextIO


class StateDictMixin:

    def state_dict(self) -> dict:
        raise NotImplementedError

    @classmethod
    def from_state_dict(cls, data: dict):
        raise NotImplementedError

    def to_pickle(self, file: Union[str, Path, BinaryIO]):
        if isinstance(file, (str, Path)):
            with open(file, "wb") as fp:
                pickle.dump(self.state_dict(), fp)
        else:
            pickle.dump(self.state_dict(), file)

    @classmethod
    def from_pickle(cls, file: Union[str, Path, BinaryIO]):
        if isinstance(file, (str, Path)):
            with open(file, "rb") as fp:
                data = pickle.load(fp)
        else:
            data = pickle.load(file)

        return cls.from_state_dict(data)

    def to_json(
            self,
            file: Union[str, Path, TextIO],
            ensure_ascii: bool = False,
            separators: Tuple[str, str] = (",", ":"),
    ):
        if isinstance(file, (str, Path)):
            with open(file, "w") as fp:
                json.dump(self.state_dict(), fp, ensure_ascii=ensure_ascii, separators=separators)
        else:
            json.dump(self.state_dict(), file, ensure_ascii=ensure_ascii, separators=separators)

    @classmethod
    def from_json(cls, file: Union[str, Path, TextIO]):
        if isinstance(file, (str, Path)):
            with open(file, "r") as fp:
                data = json.load(fp)
        else:
            data = json.load(file)

        return cls.from_state_dict(data)
