from enum import Enum
from pydantic.dataclasses import dataclass
from typing import Optional, List
from pydantic import Field, ConfigDict
import datetime
from upath import UPath
from pathlib import Path
import os
import fsspec

class RasterType(str, Enum):
    SENTINEL2 = "sentinel2"
    LANDSAT8 = "landsat"
    # TODO add more raster types as needed

class HDFType(str, Enum):
    ICESAT2 = "icesat2"
    # TODO add more HDF types as needed


class VectorType(str, Enum):
    PARQUET = "parquet"
    GEOPACKAGE = "gpkg"

@dataclass
class VectorFileName:
    name: str
    layer: Optional[str] = None
    sql_query: Optional[str] = None

    def __post_init__(self):
        if not (self.name.endswith('.gpkg') or self.name.endswith('.parquet')):
            raise ValueError(
                f"VectorFileName.name must end with '.gpkg' or '.parquet', got: {self.name}"
            )
        elif self.sql_query and self.name.endswith('.gpkg'):
            raise ValueError(
                f"Geopackage files cannot have a SQL query, got: {self.name}"
            )
        elif self.layer and self.sql_query:
            raise ValueError(
                f"VectorFileName cannot have both layer and sql_query, got: {self.name}"
            )

    @property
    def type(self) -> VectorType:
        if self.name.endswith('.gpkg'):
            return VectorType.GEOPACKAGE
        elif self.name.endswith('.parquet'):
            return VectorType.PARQUET
        else:
            raise ValueError(f"Unknown vector file type for: {self.name}. Choose either .gpkg or .parquet.")

@dataclass
class TabularFilename:
    # This is a generic name for tabular data files, such as CSV or feather files. Copy only
    name: str

@dataclass
class ParquetFileName:
    name: str
    sql_query: Optional[str] = None
    # TODO Should this be list of tuples or raw sql to open with duckdb?

@dataclass
class ReferenceFileName:
    name: str

class DirectoryType(str, Enum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    PLATINUM = "platinum"
    REFERENCE = "reference"

# === DirectoryContent ===

@dataclass(config=ConfigDict(arbitrary_types_allowed=True))
class TierRoots:
    bronze: Path | UPath | None = None
    silver: Path | UPath | None = None
    gold: Path | UPath | None = None
    platinum: Path | UPath | None = None
    reference: Path | UPath | None = None

@dataclass
class DirectoryContent:
    raster: Optional[List[str]] = Field(default=None)
    hdf: Optional[List[str]] = Field(default=None)
    vector: Optional[List[VectorFileName]] = Field(default=None)
    parquet: Optional[List[ParquetFileName]] = Field(default=None)
    table: Optional[List[TabularFilename]] = Field(default=None)


@dataclass(config=ConfigDict(arbitrary_types_allowed=True))
class DataConfig:

    date: datetime.date
    azureRoot: str | None
    aoi: str
    output_base: str
    directories: dict[DirectoryType, DirectoryContent]
    
    @property
    def fs(self) -> fsspec.AbstractFileSystem | None:
        """Get the Azure filesystem object."""
        if self.azureRoot:
            return None
        azure_account_name = os.getenv("AZURE_ACCOUNT_NAME")
        azure_account_key = os.getenv("AZURE_ACCOUNT_KEY")
        return fsspec.filesystem(
            "az",
            account_name=azure_account_name,
            account_key=azure_account_key
        )

    @property
    def tier_roots(self) -> TierRoots:
        return setup_azure_filesystem(self)


def setup_azure_filesystem(config: DataConfig) -> TierRoots:
    """
    Setup Azure filesystem and return filesystem object and base path.

    """
    azure_account_name = os.getenv("AZURE_ACCOUNT_NAME")
    azure_account_key = os.getenv("AZURE_ACCOUNT_KEY")
    if azure_account_name and azure_account_key:
        tier_roots = TierRoots()
        for directory_type in DirectoryType:
            if not config.azureRoot:
                base_path = UPath(
                    f"az://{directory_type.value}",
                    protocol="az",
                    account_name=azure_account_name,
                    account_key=azure_account_key
                )
            else:
                base_path = Path(config.azureRoot).joinpath(directory_type.value)
            setattr(tier_roots, directory_type.value, base_path)
        return tier_roots
    else:
        raise ValueError(
            "Azure account name and key must be set in environment variables AZURE_ACCOUNT_NAME and AZURE_ACCOUNT_KEY."
        )