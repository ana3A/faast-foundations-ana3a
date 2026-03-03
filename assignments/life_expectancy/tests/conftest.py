"""Pytest configuration file"""
import pandas as pd
import pytest

from life_expectancy.loaders import JSONLoader, TSVLoader
from . import FIXTURES_DIR


@pytest.fixture(scope="session")
def pt_life_expectancy_expected() -> pd.DataFrame:
    """Fixture to load the expected output of the cleaning script"""
    return pd.read_csv(FIXTURES_DIR / "pt_life_expectancy_expected.csv")


@pytest.fixture(scope="session")
def eu_life_expectancy_expected() -> pd.DataFrame:
    """Fixture to load the expected output with all EU countries"""
    return pd.read_csv(FIXTURES_DIR / "eu_life_expectancy_expected.csv")


@pytest.fixture(scope="session")
def eu_life_expectancy_raw() -> pd.DataFrame:
    """Fixture: raw TSV file loaded as-is (wide format, unparsed)."""
    return pd.read_csv(FIXTURES_DIR / "eu_life_expectancy_raw.tsv", sep="\t")


@pytest.fixture(scope="session")
def eu_life_expectancy_long() -> pd.DataFrame:
    """Fixture: TSV data parsed to long format via TSVLoader."""
    return TSVLoader().load_data(FIXTURES_DIR / "eu_life_expectancy_raw.tsv")


@pytest.fixture(scope="session")
def eu_life_expectancy_json_long() -> pd.DataFrame:
    """Fixture: JSON (zipped) data parsed to long format via JSONLoader."""
    return JSONLoader().load_data(FIXTURES_DIR / "eu_life_expectancy_raw.zip")
