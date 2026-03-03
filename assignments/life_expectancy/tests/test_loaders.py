"""Tests for the loaders module (Strategy pattern implementations)."""
import json
import zipfile
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from life_expectancy.loaders import AbstractLoader, JSONLoader, TSVLoader
from . import FIXTURES_DIR


# ---------------------------------------------------------------------------
# AbstractLoader
# ---------------------------------------------------------------------------

def test_abstract_loader_cannot_be_instantiated():
    """AbstractLoader must be an ABC and cannot be instantiated directly."""
    with pytest.raises(TypeError):
        AbstractLoader()  # type: ignore[abstract]


# ---------------------------------------------------------------------------
# TSVLoader
# ---------------------------------------------------------------------------

class TestTSVLoader:
    """Unit tests for TSVLoader."""

    def test_returns_dataframe(self):
        """TSVLoader.load_data must return a DataFrame."""
        df = TSVLoader().load_data(FIXTURES_DIR / "eu_life_expectancy_raw.tsv")
        assert isinstance(df, pd.DataFrame)

    def test_output_columns(self):
        """TSVLoader output must have the standardised six columns."""
        df = TSVLoader().load_data(FIXTURES_DIR / "eu_life_expectancy_raw.tsv")
        assert list(df.columns) == ["unit", "sex", "age", "region", "year", "value"]

    def test_year_is_int(self):
        """TSVLoader must produce integer years."""
        df = TSVLoader().load_data(FIXTURES_DIR / "eu_life_expectancy_raw.tsv")
        assert df["year"].dtype in ["int64", "int32"]

    def test_value_is_float(self):
        """TSVLoader must produce float values."""
        df = TSVLoader().load_data(FIXTURES_DIR / "eu_life_expectancy_raw.tsv")
        assert df["value"].dtype == "float64"

    def test_no_missing_values(self):
        """TSVLoader must drop missing/sentinel values."""
        df = TSVLoader().load_data(FIXTURES_DIR / "eu_life_expectancy_raw.tsv")
        assert not df["value"].isna().any()

    def test_no_flag_chars_in_values(self):
        """TSVLoader must strip trailing flag letters from numeric values."""
        df = TSVLoader().load_data(FIXTURES_DIR / "eu_life_expectancy_raw.tsv")
        # All values should be convertible to float (no stray letters)
        for val in df["value"]:
            assert isinstance(val, float)

    def test_regions_are_strings(self):
        """Region column must contain plain strings."""
        df = TSVLoader().load_data(FIXTURES_DIR / "eu_life_expectancy_raw.tsv")
        assert df["region"].dtype == object

    def test_known_regions_present(self):
        """Fixture TSV contains data for DE, DE_TOT, FR and PT."""
        df = TSVLoader().load_data(FIXTURES_DIR / "eu_life_expectancy_raw.tsv")
        assert set(df["region"].unique()) == {"DE", "DE_TOT", "FR", "PT"}


# ---------------------------------------------------------------------------
# JSONLoader
# ---------------------------------------------------------------------------

class TestJSONLoader:
    """Unit tests for JSONLoader."""

    def test_returns_dataframe_from_zip(self):
        """JSONLoader must parse a zipped JSON file into a DataFrame."""
        df = JSONLoader().load_data(FIXTURES_DIR / "eu_life_expectancy_raw.zip")
        assert isinstance(df, pd.DataFrame)

    def test_returns_dataframe_from_plain_json(self):
        """JSONLoader must also work with a plain (unzipped) JSON file."""
        df = JSONLoader().load_data(FIXTURES_DIR / "eu_life_expectancy_raw.json")
        assert isinstance(df, pd.DataFrame)

    def test_output_columns(self):
        """JSONLoader output must have the standardised six columns."""
        df = JSONLoader().load_data(FIXTURES_DIR / "eu_life_expectancy_raw.zip")
        assert list(df.columns) == ["unit", "sex", "age", "region", "year", "value"]

    def test_year_is_int(self):
        """JSONLoader must produce integer years."""
        df = JSONLoader().load_data(FIXTURES_DIR / "eu_life_expectancy_raw.zip")
        assert df["year"].dtype in ["int64", "int32"]

    def test_value_is_float(self):
        """JSONLoader must produce float values."""
        df = JSONLoader().load_data(FIXTURES_DIR / "eu_life_expectancy_raw.zip")
        assert df["value"].dtype == "float64"

    def test_no_missing_values(self):
        """JSONLoader sparse format must not introduce NaN values."""
        df = JSONLoader().load_data(FIXTURES_DIR / "eu_life_expectancy_raw.zip")
        assert not df["value"].isna().any()

    def test_known_regions_present(self):
        """Fixture JSON contains data for DE, DE_TOT, FR and PT."""
        df = JSONLoader().load_data(FIXTURES_DIR / "eu_life_expectancy_raw.zip")
        assert set(df["region"].unique()) == {"DE", "DE_TOT", "FR", "PT"}

    def test_json_and_tsv_produce_same_values_for_pt(self):
        """TSVLoader and JSONLoader must produce identical PT data."""
        tsv_df = TSVLoader().load_data(FIXTURES_DIR / "eu_life_expectancy_raw.tsv")
        json_df = JSONLoader().load_data(FIXTURES_DIR / "eu_life_expectancy_raw.zip")

        cols = ["unit", "sex", "age", "region", "year", "value"]
        sort_keys = ["unit", "sex", "age", "region", "year"]

        tsv_pt = (
            tsv_df[tsv_df["region"] == "PT"][cols]
            .sort_values(sort_keys)
            .reset_index(drop=True)
        )
        json_pt = (
            json_df[json_df["region"] == "PT"][cols]
            .sort_values(sort_keys)
            .reset_index(drop=True)
        )

        pd.testing.assert_frame_equal(tsv_pt, json_pt)

    def test_invalid_zip_raises(self):
        """JSONLoader must raise an error for a zip without a JSON entry."""
        with tempfile.TemporaryDirectory() as tmpdir:
            bad_zip = Path(tmpdir) / "bad.zip"
            with zipfile.ZipFile(bad_zip, "w") as zf:
                zf.writestr("data.txt", "not json")

            with pytest.raises(StopIteration):
                JSONLoader().load_data(bad_zip)
