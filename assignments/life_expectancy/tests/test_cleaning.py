"""Tests for the cleaning module"""
import tempfile
from pathlib import Path
from unittest.mock import patch
import pandas as pd
import pytest

from life_expectancy.cleaning import (
    clean_data,
    clean_data_pipeline,
    load_data,
    save_data,
)
from life_expectancy.loaders import JSONLoader, TSVLoader
from life_expectancy.regions import Region
from . import FIXTURES_DIR


# ---------------------------------------------------------------------------
# Helpers – small long-format DataFrames for unit tests
# ---------------------------------------------------------------------------

def _make_long_df(geos: list[str], years: list[int], values: list[float]) -> pd.DataFrame:
    """Build a minimal long-format DataFrame for testing."""
    rows = []
    for geo, year, value in zip(geos, years, values):
        rows.append({"unit": "YR", "sex": "F", "age": "Y65",
                     "region": geo, "year": year, "value": value})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Integration tests – use fixture data
# ---------------------------------------------------------------------------

def test_clean_data(pt_life_expectancy_expected, eu_life_expectancy_long):
    """clean_data with long-format TSV data should match the expected CSV."""
    cleaned = clean_data(eu_life_expectancy_long, region=Region.PT)

    pd.testing.assert_frame_equal(
        cleaned.reset_index(drop=True), pt_life_expectancy_expected
    )


def test_clean_data_with_custom_region(eu_life_expectancy_expected, eu_life_expectancy_long):
    """clean_data with Region.FR should return only French rows."""
    region = Region.FR
    actual = clean_data(eu_life_expectancy_long, region=region)
    expected = (
        eu_life_expectancy_expected[eu_life_expectancy_expected["region"] == region.value]
        .reset_index(drop=True)
    )
    pd.testing.assert_frame_equal(actual.reset_index(drop=True), expected)


# ---------------------------------------------------------------------------
# Unit tests – load_data
# ---------------------------------------------------------------------------

def test_load_data_tsv():
    """load_data with no explicit loader should use TSVLoader and produce long format."""
    df = load_data(FIXTURES_DIR / "eu_life_expectancy_raw.tsv")

    assert not df.empty
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["unit", "sex", "age", "region", "year", "value"]
    assert df["year"].dtype in ["int64", "int32"]
    assert df["value"].dtype == "float64"


def test_load_data_accepts_explicit_tsv_loader():
    """load_data should accept an explicit TSVLoader instance."""
    df = load_data(FIXTURES_DIR / "eu_life_expectancy_raw.tsv", loader=TSVLoader())
    assert not df.empty
    assert "region" in df.columns


def test_load_data_accepts_json_loader():
    """load_data with JSONLoader should parse a zip file into long format."""
    df = load_data(FIXTURES_DIR / "eu_life_expectancy_raw.zip", loader=JSONLoader())

    assert not df.empty
    assert list(df.columns) == ["unit", "sex", "age", "region", "year", "value"]
    assert df["year"].dtype in ["int64", "int32"]
    assert df["value"].dtype == "float64"


# ---------------------------------------------------------------------------
# Unit tests – clean_data
# ---------------------------------------------------------------------------

def test_clean_data_filters_region():
    """clean_data must keep only rows for the requested region."""
    df = _make_long_df(["PT", "FR", "DE"], [2020, 2020, 2020], [21.5, 23.4, 20.8])
    result = clean_data(df, region=Region.FR)

    assert len(result) == 1
    assert result["region"].iloc[0] == "FR"
    assert result["value"].iloc[0] == 23.4


def test_clean_data_returns_reset_index():
    """clean_data must return a DataFrame with a fresh integer index."""
    df = _make_long_df(["PT", "FR"], [2020, 2020], [21.5, 23.4])
    result = clean_data(df, region=Region.PT)

    assert result.index.tolist() == list(range(len(result)))


def test_clean_data_default_region_is_pt():
    """clean_data's default region is PT."""
    df = _make_long_df(["PT", "FR"], [2020, 2020], [21.5, 23.4])
    result = clean_data(df)  # no region argument

    assert all(result["region"] == "PT")


# ---------------------------------------------------------------------------
# Unit tests – save_data
# ---------------------------------------------------------------------------

def test_save_data_calls_to_csv():
    """save_data should call to_csv with the correct arguments."""
    df = _make_long_df(["PT"], [2020], [21.5])

    with tempfile.TemporaryDirectory() as tmpdir:
        output_file = Path(tmpdir) / "test_output.csv"

        with patch.object(pd.DataFrame, "to_csv") as mock_to_csv:
            save_data(df, output_file)
            mock_to_csv.assert_called_once_with(output_file, index=False)


def test_save_data_creates_parent_dirs():
    """save_data must create missing parent directories."""
    df = _make_long_df(["PT"], [2020], [21.5])

    with tempfile.TemporaryDirectory() as tmpdir:
        output_file = Path(tmpdir) / "nested" / "dirs" / "out.csv"
        assert not output_file.parent.exists()

        with patch.object(pd.DataFrame, "to_csv"):
            save_data(df, output_file)

        assert output_file.parent.exists()


def test_save_data_index_false():
    """save_data must always write without the DataFrame index."""
    df = _make_long_df(["PT"], [2020], [21.5])

    with tempfile.TemporaryDirectory() as tmpdir:
        output_file = Path(tmpdir) / "out.csv"

        with patch.object(pd.DataFrame, "to_csv") as mock_to_csv:
            save_data(df, output_file)

        call_kwargs = mock_to_csv.call_args[1]
        assert call_kwargs.get("index") is False


# ---------------------------------------------------------------------------
# End-to-end pipeline tests
# ---------------------------------------------------------------------------
# The pipeline reads from the full production data file (all regions/ages),
# so we verify structural correctness rather than exact equality with the
# small fixture expected CSV.

def _assert_pipeline_result(result: pd.DataFrame, region: Region = Region.PT) -> None:
    """Common structural assertions for pipeline output."""
    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert list(result.columns) == ["unit", "sex", "age", "region", "year", "value"]
    assert result["year"].dtype in ["int64", "int32"]
    assert result["value"].dtype == "float64"
    assert not result["value"].isna().any()
    # All rows must belong to the requested region
    assert (result["region"] == region.value).all()


def test_clean_data_pipeline_tsv():
    """End-to-end: TSVLoader pipeline must return only PT rows with correct types."""
    result = clean_data_pipeline(region=Region.PT)
    _assert_pipeline_result(result, Region.PT)


def test_clean_data_pipeline_json():
    """End-to-end: JSONLoader pipeline must return PT rows identical to TSVLoader."""
    tsv_result  = clean_data_pipeline(region=Region.PT)
    json_result = clean_data_pipeline(region=Region.PT, loader=JSONLoader())

    _assert_pipeline_result(json_result, Region.PT)

    # Both loaders must agree on the shape and values for PT
    cols = ["unit", "sex", "age", "region", "year", "value"]
    sort_keys = ["unit", "sex", "age", "year"]

    tsv_sorted  = tsv_result[cols].sort_values(sort_keys).reset_index(drop=True)
    json_sorted = json_result[cols].sort_values(sort_keys).reset_index(drop=True)

    pd.testing.assert_frame_equal(tsv_sorted, json_sorted)

