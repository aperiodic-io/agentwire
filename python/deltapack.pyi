"""DPK1 (SPEC.md §7): LLM request logs and trace spans stored as deltas against earlier records."""

from collections.abc import Iterator
from os import PathLike
from typing import Optional, Union

_Bytes = Union[bytes, bytearray]
_Path = Union[str, PathLike[str]]

class Writer:
    """Appends records to a new DPK1 file (never an existing one: FileExistsError)."""

    def __init__(self, path: _Path, window: int = 16, key: Optional[str] = None) -> None:
        """With `key` (64 hex digits), every record is sealed with it."""
    def append(self, body: _Bytes, meta: Optional[_Bytes] = None) -> int:
        """Appends `body` with its `meta` (stored as is) and returns the record's index."""
    def flush(self) -> None: ...
    def close(self) -> None: ...
    def __enter__(self) -> "Writer": ...
    def __exit__(self, *exc: object) -> bool: ...

class Reader:
    """Reads a DPK1 file; a record that does not rebuild the body it names raises ValueError."""

    def __init__(self, path: _Path, max_body: int = 64 << 20, key: Optional[str] = None) -> None:
        """`key` (64 hex digits) reads a sealed file: without it, or with another, ValueError."""
    @property
    def sealed(self) -> bool:
        """The file's records are sealed: reading them needs its key."""
    def __len__(self) -> int: ...
    @property
    def truncated(self) -> bool:
        """The file ends inside a record: the records before it are whole."""
    def get(self, i: int) -> bytes: ...
    def __getitem__(self, i: int) -> bytes: ...
    def meta(self, i: int) -> bytes: ...
    def records(self) -> list[tuple[bytes, int, int]]:
        """(meta, back, stored) per record: back is how far back its base is (0: stored whole)."""
    def __iter__(self) -> "Records": ...

class Records(Iterator[tuple[bytes, bytes]]):
    """(meta, body) for every record, in order, each rebuilt once."""

    def __next__(self) -> tuple[bytes, bytes]: ...
