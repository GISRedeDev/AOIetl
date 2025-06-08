import pytest

from aoietl.build_paths import build_config
from aoietl.validation import validate_directories


def test_validate_directories(test_config):
    config = build_config(test_config)
    try:
        validate_directories(config)
    except ValueError as e:
        pytest.fail(f"Validation failed with error: {e}")

    assert True