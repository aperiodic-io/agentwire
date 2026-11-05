from __future__ import annotations

import pytest
from agentwire.fixtures import training_samples
from agentwire.transport import DictionaryRegistry
from agentwire.zstd import train_dictionary


@pytest.fixture(scope="session")
def dictionaries() -> tuple[bytes, bytes]:
    return (train_dictionary(training_samples(7), 8192),
            train_dictionary(training_samples(99), 8192))


@pytest.fixture
def registry(dictionaries) -> DictionaryRegistry:
    reg = DictionaryRegistry()
    reg.register(1, dictionaries[0])
    reg.register(2, dictionaries[1])
    return reg
