from pathlib import Path

from .data_types import DataConfig


def copy_files(config: DataConfig) -> None:
    """
    Copy files from the source directories to the output directory based on the configuration.

    Args:
        config (DataConfig): The configuration object containing paths and directory structure.
    """
    if not config.outputBase:
        raise ValueError("Output base directory is not specified in the configuration.")

    # Implement file copying logic here
    # This is a placeholder for actual file copying logic
    print(f"Copying files to {config.outputBase}...")  # Replace with actual copy logic