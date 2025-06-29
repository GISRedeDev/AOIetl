from pathlib import Path
import os
from typing import Union

import fsspec
from upath import UPath

from .data_types import DataConfig, DirectoryType


def setup_azure_filesystem(config: DataConfig, container: DirectoryType) -> tuple[Union[fsspec.AbstractFileSystem, None], UPath]:
    """
    Setup Azure filesystem and return filesystem object and base path.
    """
    azure_account_name = os.getenv("AZURE_ACCOUNT_NAME")
    azure_account_key = os.getenv("AZURE_ACCOUNT_KEY")
    if azure_account_name and azure_account_key:
        fs = fsspec.filesystem(
            "az",
            account_name=azure_account_name,
            account_key=azure_account_key
        )
        base_path = UPath(
            f"az://{container.value}",
            protocol="az",
            account_name=azure_account_name,
            account_key=azure_account_key
        )
        return fs, base_path
    else:
        raise ValueError(
            "Azure account name and key must be set in environment variables AZURE_ACCOUNT_NAME and AZURE_ACCOUNT_KEY."
        )