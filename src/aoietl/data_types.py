from enum import Enum
from pydantic.dataclasses import dataclass
from typing import Optional, List
from pydantic import Field
import datetime


class RasterType(str, Enum):
    SENTINEL2 = "sentinel-2"
    LANDSAT8 = "landsat"
    # TODO add more raster types as needed

class HDFType(str, Enum):
    ICESAT2 = "icesat-2"
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

# === DirectoryContent ===

@dataclass
class DirectoryContent:
    raster: Optional[List[str]] = Field(default=None)
    hdf: Optional[List[str]] = Field(default=None)
    vector: Optional[List[VectorFileName]] = Field(default=None)
    parquet: Optional[List[ParquetFileName]] = Field(default=None)
    table: Optional[List[TabularFilename]] = Field(default=None)


@dataclass
class DataConfig:
    date: datetime.date
    azureRoot: str
    aoi: str
    output_base: str
    directories: dict[DirectoryType, DirectoryContent]