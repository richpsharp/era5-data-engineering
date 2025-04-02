"""This module includes functions to support file operations on Databricks."""

import os
import hashlib


def copy_file_with_hash(src_file, target_directory):
    """Copies file with SHA-256 hash appended to its filename.

    Reads a file into memory, computes its SHA-256 hash, and writes the file
    to the target directory with the hash appended to the filename. The new
    filename format is:
      originalname_<hash>.ext

    The new filename format is:
      originalname_<hash>.ext

    Args:
        src_file (str): Path to the source file.
        target_directory (str): Destination directory path (should exist or be
            created).

    Returns:
        tuple: (new_file_path, file_hash)
    """
    # Read the entire file into memory
    with open(src_file, "rb") as f:
        file_data = f.read()

    file_hash = hashlib.sha256(file_data).hexdigest()

    # create new path with the hash on the file
    base_name = os.path.basename(src_file)
    name, ext = os.path.splitext(base_name)
    new_file_name = f"{name}_{file_hash}{ext}"
    dest_file = os.path.join(target_directory, new_file_name)

    with open(dest_file, "wb") as f:
        f.write(file_data)

    return dest_file, file_hash
