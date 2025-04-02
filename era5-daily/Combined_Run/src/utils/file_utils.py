"""Module for supporting file operations on Databricks."""

import io
import hashlib
import logging

import xarray as xr

LOGGER = logging.getLogger(__name__)


def is_netcdf_file_valid(file_binary, source_file_path):
    """Check if a NetCDF binary file can be opened with xarray.

    Args:
        file_binary (bytes): Binary content of the NetCDF file.
        source_file_path (str): Keep this for logging.

    Returns:
        bool: True if the file is valid, False if corrupt.
    """
    try:
        with xr.open_dataset(io.BytesIO(file_binary)) as ds:
            # this will raise an exception if it's wrong
            ds.load()
        return True
    except Exception:
        LOGGER.exception(
            f"something bad happend on when trying to read {source_file_path}"
        )
        return False


def copy_file_to_mem(source_path):
    """Copy file contents to memory.

    Reads the file at the specified source path in binary mode and returns its contents.

    Args:
        source_path (str): Path to the source file.

    Returns:
        bytes: The binary content of the file.
    """
    with open(source_path, "rb") as f:
        file_data = f.read()
    return file_data


def hash_bytes(file_data):
    """Compute the SHA-256 hash of the given file data.

    Args:
        file_data (bytes): The binary content of the file.

    Returns:
        str: The hexadecimal SHA-256 hash.
    """
    file_hash = hashlib.sha256(file_data).hexdigest()
    return file_hash


def copy_mem_file_to_path(file_data, target_path):
    """Write binary file data to the specified target path.

    Args:
        file_data (bytes): The binary file data.
        target_path (str): Destination file path.

    Returns:
        None
    """
    with open(target_path, "wb") as f:
        f.write(file_data)
