"""Trigger script to run PyTest so it can be run in DataBricks."""

import pytest
import sys

if __name__ == "__main__":
    # Arguments for pytest
    pytest_args = [
        "--log-cli-level=DEBUG",
        "--maxfail=1",
        "tests/test_era5_01_bronze_source_to_staging.py",
    ]
    sys.exit(pytest.main(pytest_args))
