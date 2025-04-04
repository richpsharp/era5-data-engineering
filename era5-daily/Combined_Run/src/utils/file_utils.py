"""Module for supporting file operations on Databricks."""

import hashlib
import logging

import netCDF4

LOGGER = logging.getLogger(__name__)


def is_netcdf_file_valid(source_file_path):
    """Check if a NetCDF file is valid by opening its header with netCDF4.

    Args:
        source_file_path (str): The path to the NetCDF file.

    Returns:
        bool: True if the file is valid, False if there's any failure.
    """
    try:
        # Open the file in read-only mode; this only reads the header.
        ds = netCDF4.Dataset(source_file_path, mode="r")
        ds.close()
        return True
    except Exception:
        LOGGER.exception(f"Error reading NetCDF header from {source_file_path}")
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
