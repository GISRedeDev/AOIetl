
from .data_types import (
    DataConfig,
    RasterType,
    HDFType,
    VectorFileName,
    ParquetFileName,
)


def validate_directories(data_config: DataConfig):
    for tier, content in data_config.directories.items():

        if content.raster:
            for r in content.raster:
                try:
                    RasterType(r)
                except ValueError:
                    raise ValueError(f"Invalid raster type '{r}' in tier '{tier.value}'")
                
        if content.hdf:
            for h in content.hdf:
                try:
                    HDFType(h)
                except ValueError:
                    raise ValueError(f"Invalid HDF type '{h}' in tier '{tier.value}'")

        if content.vector:
            for v in content.vector:
                if not isinstance(v, VectorFileName) or not v.name:
                    raise ValueError(f"Invalid vector type '{v}' in tier '{tier.value}'")

        if content.parquet:
            for p in content.parquet:
                if not isinstance(p, ParquetFileName) or not p.name:
                    raise ValueError(f"Invalid parquet type '{p}' in tier '{tier.value}'")
    return True