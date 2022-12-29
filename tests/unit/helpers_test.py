"""Unit tests for the energy_comms.helpers module."""

import logging
import pathlib

import geopandas
import pandas as pd
from geopandas.testing import assert_geodataframe_equal
from shapely.geometry import Point

import energy_comms

logger = logging.getLogger(__name__)


def _check_adjacent_id_fips(actual_ser: pd.Series, expected_ser: pd.Series) -> None:
    expected = expected_ser.explode().sort_values().to_list()
    actual = actual_ser.explode().sort_values().to_list()
    if actual != expected:
        raise AssertionError("Actual adjacent_id_fips column doesn't match expected.")


def test_geometry_intersections(test_dir: pathlib.Path) -> None:
    """Test if geometry intersection helper functions work correctly.

    Performs test on one record and adds geometries at the Census
    tract level.
    """
    test_gdf = geopandas.GeoDataFrame(
        [
            [
                1,
                "UT",
                -111.121944,
                39.297500,
                Point(-111.121944, 39.297500),
            ]
        ],
        columns=[
            "mine_id",
            "state",
            "longitude",
            "latitude",
            "geometry",
        ],
        crs="EPSG:4269",
    )

    census_gdf = pd.read_pickle(test_dir / "test_inputs/utah_census_tracts_gdf.pkl.gz")

    expected_gdf = geopandas.GeoDataFrame(
        [
            [
                1,
                "UT",
                -111.121944,
                39.297500,
                Point(-111.121944, 39.297500),
                "49015976300",
                "Census Tract 9763",
                ["49015976200", "49015976500", "49039972100", "49039972500"],
            ]
        ],
        columns=[
            "mine_id",
            "state",
            "longitude",
            "latitude",
            "geometry",
            "tract_id_fips",
            "tract_name_census",
            "adjacent_id_fips",
        ],
        crs="EPSG:4269",
    )

    actual = energy_comms.helpers.get_geometry_intersection(
        test_gdf,
        census_geometry="tract",
        add_adjacent_geoms=True,
        census_gdf=census_gdf,
    )
    # can't hash lists in checking df equality, check separately
    _check_adjacent_id_fips(
        actual["adjacent_id_fips"], expected_gdf["adjacent_id_fips"]
    )
    actual = actual.drop(columns=["adjacent_id_fips"])
    expected_gdf = expected_gdf.drop(columns=["adjacent_id_fips"])

    assert_geodataframe_equal(actual, expected_gdf, check_dtype=False)


def test_invalid_lat_lon_range() -> None:
    """Test if invalid latitude and longitude filter works."""
    test_df = pd.DataFrame(
        {"latitude": [1.1, 2.3, -90.5, None], "longitude": [2.9, 181.1, -180.1, 3.5]}
    )
    expected_df = pd.DataFrame({"latitude": [1.1, None], "longitude": [2.9, 3.5]})
    actual_df = energy_comms.helpers.remove_invalid_lat_lon_records(
        test_df
    ).reset_index(drop=True)
    pd.testing.assert_frame_equal(expected_df, actual_df)
