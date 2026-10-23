from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from tests.helpers import FIXTURES


@pytest.fixture
def fixtures_dir() -> Path:
    return FIXTURES


@pytest.fixture
def order_placed_payload() -> dict[str, Any]:
    return json.loads((FIXTURES / "valid_order_placed.json").read_text())
