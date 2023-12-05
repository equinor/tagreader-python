from pathlib import Path
from typing import Generator

import pytest

from tagreader.cache import SmartCache


@pytest.fixture  # type: ignore[misc]
def cache(tmp_path: Path) -> Generator[SmartCache, None, None]:
    cache = SmartCache(directory=tmp_path, size_limit=int(4e9))
    yield cache
