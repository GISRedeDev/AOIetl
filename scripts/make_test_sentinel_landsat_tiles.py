"""
This script generates 25 tiles (5x5 grid) of Sentinel-2 and Landsat data for testing purposes. It does not hold any real data.
It's just meant to represent the structure of the data and how it would be used in a real scenario.

INPUT:
I takes any existing Sentinel-2 tile and generates the tiles for 2 days for both Sentinel-2 and Landsat.
"""
import rasterio
from rasterio.windows import Window
import numpy as np
import os
from pathlib import Path
from datetime import datetime, timedelta


def sentinel_filename(date: datetime, mgrs_tile: str, orbit="R117", platform="S2A"):
    acq_time = date.strftime("%Y%m%dT015631")
    proc_time = date.strftime("%Y%m%dT043813")
    return f"{platform}_MSIL2A_{acq_time}_{orbit}_{mgrs_tile}_{proc_time}.tif"

# Landsat filename template
def landsat_filename(date: datetime, pathrow="120034", sensor="LC08"):
    acq_date = date.strftime("%Y%m%d")
    return f"{sensor}_L2SP_{pathrow}_{acq_date}_02_T1.tif"


def main(src_path: Path, sentinel_output_dir: Path, landsat_output_dir: Path):
    with rasterio.open(src_path) as src:
        tile_width = src.width // 5
        tile_height = src.height // 5

        mgrs_tile_code = "T51LWC"  # you can change this if you want
        landsat_pathrow = "120034" # change this if needed

        base_date_sentinel = datetime.strptime("20250401", "%Y%m%d")
        base_date_landsat = datetime.strptime("20250422", "%Y%m%d")

        for day_offset in [0, 1]:  # Day 1 and Day 2
            date_sentinel = base_date_sentinel + timedelta(days=day_offset)
            date_landsat = base_date_landsat + timedelta(days=day_offset)

            for row in range(5):
                for col in range(5):
                    window = Window(
                        col * tile_width,
                        row * tile_height,
                        tile_width,
                        tile_height
                    )
                    transform = src.window_transform(window)

                    # Dummy data
                    dummy_data = np.ones((tile_height, tile_width), dtype=np.uint8)

                    # Meta for output
                    tile_meta = src.meta.copy()
                    tile_meta.update({
                        "driver": "GTiff",
                        "height": tile_height,
                        "width": tile_width,
                        "transform": transform,
                        "dtype": "uint8",
                        "count": 1,
                        "compress": "DEFLATE",
                        "predictor": 2,
                        "tiled": True
                    })

                    # SENTINEL filename and write
                    sentinel_fname = sentinel_filename(date_sentinel, mgrs_tile_code)
                    sentinel_tile_path = sentinel_output_dir.joinpath(                        f"{sentinel_fname[:-4]}_tile{row}{col}.tif"
                    )
                    with rasterio.open(sentinel_tile_path, "w", **tile_meta) as dst:
                        dst.write(dummy_data, 1)

                    # LANDSAT filename and write
                    landsat_fname = landsat_filename(date_landsat, landsat_pathrow)
                    landsat_tile_path = landsat_output_dir.joinpath(
                        f"{landsat_fname[:-4]}_tile{row}{col}.tif"
                    )
                    with rasterio.open(landsat_tile_path, "w", **tile_meta) as dst:
                        dst.write(dummy_data, 1)


if __name__ == "__main__":
    BASE = Path(__file__).parent.parent.resolve().joinpath("tests", "data")
    INPUT_RASTER = BASE.joinpath("S2A_MSIL2A_20250401T015631_R117_T51LWC_20250401T043813.tif")
    sentinel_output_dir = BASE.joinpath("sentinel")
    landsat_output_dir = BASE.joinpath("landsat")
    if not sentinel_output_dir.exists():
        sentinel_output_dir.mkdir(parents=True, exist_ok=True)
    if not landsat_output_dir.exists():
        landsat_output_dir.mkdir(parents=True, exist_ok=True)
    main(INPUT_RASTER, sentinel_output_dir, landsat_output_dir)

