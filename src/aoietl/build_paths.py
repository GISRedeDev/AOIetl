from pathlib import Path
import yaml

from .data_types import DataConfig

def build_config(config_yaml_path: str | Path) -> DataConfig:
    """
    Build a DataConfig object from a given path.

    Args:
        path (str | Path): The path to the data directory.

    Returns:
        DataConfig: An instance of DataConfig with the specified path.
    """
    with open(config_yaml_path, 'r') as file:
        config = yaml.safe_load(file)

    if data_config := config.get('dataConfig'):
        return DataConfig(**data_config)

    raise ValueError("Invalid config format (no `dataConfig` element in yaml). Please check the yaml structure.")
