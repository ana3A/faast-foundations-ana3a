"""Module for cleaning life expectancy data"""
import argparse
from pathlib import Path

import pandas as pd

from life_expectancy.loaders import AbstractLoader, TSVLoader
from life_expectancy.regions import Region


def load_data(file_path: Path, loader: AbstractLoader | None = None) -> pd.DataFrame:
    """Load life expectancy data using a loader strategy.

    When no *loader* is provided the default :class:`~life_expectancy.loaders.TSVLoader`
    is used, preserving backward-compatible behaviour with the original TSV
    data source.

    Args:
        file_path: Path to the source data file.
        loader: A concrete :class:`~life_expectancy.loaders.AbstractLoader`
            instance.  Defaults to ``TSVLoader()``.

    Returns:
        Long-format DataFrame with columns
        ``unit``, ``sex``, ``age``, ``region``, ``year``, ``value``.
    """
    if loader is None:
        loader = TSVLoader()
    return loader.load_data(file_path)


def clean_data(df: pd.DataFrame, region: Region = Region.PT) -> pd.DataFrame:
    """Filter a long-format life expectancy DataFrame by region.

    The *df* argument must already be in the standardised long format produced
    by any :class:`~life_expectancy.loaders.AbstractLoader` implementation
    (columns: ``unit``, ``sex``, ``age``, ``region``, ``year``, ``value``).

    Args:
        df: Long-format DataFrame returned by :func:`load_data`.
        region: The target region to keep (default: :attr:`Region.PT`).

    Returns:
        Filtered DataFrame containing only rows for *region*.
    """
    return df[df["region"] == region.value].reset_index(drop=True)


def save_data(df: pd.DataFrame, output_file: Path) -> None:
    """Save the cleaned data to a CSV file.

    Args:
        df: Cleaned DataFrame to save.
        output_file: Destination path for the output CSV.
    """
    output_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_file, index=False)


def clean_data_pipeline(
    region: Region = Region.PT,
    loader: AbstractLoader | None = None,
) -> pd.DataFrame:
    """Execute the complete data cleaning pipeline.

    Loads data from the ``life_expectancy/data`` directory using the
    provided *loader* strategy (defaulting to :class:`~life_expectancy.loaders.TSVLoader`),
    filters by *region*, saves the result to a CSV, and returns the cleaned
    DataFrame.

    Args:
        region: The region to filter (default: :attr:`Region.PT`).
        loader: Loader strategy to use.  Pass a
            :class:`~life_expectancy.loaders.JSONLoader` to read from the
            zipped JSON file instead of the default TSV.

    Returns:
        Cleaned DataFrame.
    """
    data_dir = Path(__file__).parent / "data"

    if loader is None or isinstance(loader, TSVLoader):
        loader = loader or TSVLoader()
        input_file = data_dir / "eu_life_expectancy_raw.tsv"
    else:
        # Any other loader (e.g. JSONLoader) reads from the zipped JSON
        input_file = data_dir / "eu_life_expectancy_raw.zip"

    output_file = data_dir / f"{region.value.lower()}_life_expectancy.csv"

    raw_data = load_data(input_file, loader)
    cleaned_data = clean_data(raw_data, region)
    save_data(cleaned_data, output_file)

    return cleaned_data


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser(description="Clean life expectancy data")
    parser.add_argument(
        "--region",
        type=str,
        default="PT",
        help="Region code to filter (default: PT)",
    )
    args = parser.parse_args()
    region_enum = Region[args.region.upper()]
    clean_data_pipeline(region=region_enum)
