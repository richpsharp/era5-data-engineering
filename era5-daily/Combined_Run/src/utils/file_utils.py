"""Module for supporting file operations on Databricks."""

import hashlib
import logging

import xarray as xr

LOGGER = logging.getLogger(__name__)


def is_netcdf_file_valid(source_file_path):
    """Check if a NetCDF binary file can be opened with xarray.

    Args:
        source_file_path (str): Keep this for logging.

    Returns:
        bool: True if the file is valid, False if corrupt.
    """
    try:
        with xr.open_dataset(source_file_path, engine="netcdf4") as ds:
            # this will raise an exception if it can't open it as a netcdf4
            ds.load()
        return True
    except Exception:
        LOGGER.exception(
            f"something bad happend on when trying to read {source_file_path}"
        )
        return False


def hash_file(file_path):
    """Compute the SHA-256 hash of the given filepath.

    Args:
        file_path (str): Path to a file.

    Returns:
        str: The hexadecimal SHA-256 hash.
    """
    with open(file_path, "rb") as file:
        file_hash = hashlib.sha256(file.read()).hexdigest()
    return file_hash
