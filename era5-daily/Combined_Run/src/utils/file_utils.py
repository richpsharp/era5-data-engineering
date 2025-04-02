"""Module for supporting file operations on Databricks."""

import hashlib


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
