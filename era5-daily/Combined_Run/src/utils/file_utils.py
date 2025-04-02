"""Module for supporting file operations on Databricks."""

import tempfile
import os
import io
import hashlib
import logging

import xarray as xr

LOGGER = logging.getLogger(__name__)
TEMPFILE_ROOT = "/local_disk0"

if not os.path.exists(TEMPFILE_ROOT):
    LOGGER.warning(
        f"{TEMPFILE_ROOT} did not exist which is a fast nvme mount on "
        f"databricks, this means netcdf validity checks will be slower by "
        "writing to /tmp"
    )
    TEMPFILE_ROOT = "/tmp"


def is_netcdf_file_valid(file_binary, source_file_path):
    """Check if a NetCDF binary file can be opened with xarray.

    Args:
        file_binary (bytes): Binary content of the NetCDF file.
        source_file_path (str): Keep this for logging.

    Returns:
        bool: True if the file is valid, False if corrupt.
    """
    try:
        with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
            tmp.write(file_binary)
            tmp.flush()
            temp_filename = tmp.name
        with xr.open_dataset(temp_filename, engine="netcdf4") as ds:
            # this will raise an exception if it can't open it as a netcdf4
            ds.load()
        return True
    except Exception:
        LOGGER.exception(
            f"something bad happend on when trying to read {source_file_path}"
        )
        return False
    finally:
        os.remove(temp_filename)


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
