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
    EXAMPLE_GPKG = "points.gpkg"
    # GPGKG = "gpkg"
    # SHAPEFILE = "shapefile"
    # TODO Will gpkgs be in folders or root? Layer names? Will there be shps?

class ParquetType(str, Enum):
    EXAMPLE_PARQUET = "points.parquet"
    BATHYMETRY_REFERENCE = "bathymetry_reference.parquet"
    # TODO Will parquet be in folders or root?

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
    vector: Optional[List[str]] = Field(default=None)
    parquet: Optional[List[str]] = Field(default=None)
    geoparquet: Optional[List[str]] = Field(default=None)


@dataclass
class DataConfig:
    date: datetime.date
    azureRoot: str
    aoi: str
    directories: dict[DirectoryType, DirectoryContent]