import os

import pytest


@pytest.fixture(scope="session")
def game_id() -> int:
    """Default game id used by API tests.

    These tests are written as smoke checks against a running local server.
    Keep this fixture stable and allow override via env var.
    """
    value = os.getenv("PWHL_TEST_GAME_ID", "105")
    try:
        return int(value)
    except ValueError:
        return 105
