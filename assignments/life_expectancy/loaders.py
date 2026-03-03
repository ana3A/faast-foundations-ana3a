"""
Data loading strategies for life expectancy data.

This module implements the Strategy design pattern: an abstract loader
interface with concrete implementations for each supported file format.
Currently supports TSV (tab-separated) and JSON (Eurostat API format,
plain or zipped).
"""
import json
import zipfile
from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd


class AbstractLoader(ABC):  # pylint: disable=too-few-public-methods
    """Abstract base class defining the loader strategy interface.

    All concrete loaders must implement :meth:`load_data`, which reads a
    file and returns a standardised long-format DataFrame with the columns:
    ``unit``, ``sex``, ``age``, ``region``, ``year``, ``value``.

    The Strategy pattern intentionally keeps each loader to a single public
    method (``load_data``).
    """

    @abstractmethod
    def load_data(self, file_path: Path) -> pd.DataFrame:
        """Load and parse data from *file_path* into a long-format DataFrame.

        Args:
            file_path: Path to the source data file.

        Returns:
            DataFrame with columns ``unit``, ``sex``, ``age``, ``region``,
            ``year`` (int), ``value`` (float).  Missing values are dropped.
        """


class TSVLoader(AbstractLoader):  # pylint: disable=too-few-public-methods
    """Strategy that loads life expectancy data from a TSV file.

    The file is expected to follow the Eurostat TSV layout where the first
    column encodes four dimensions as ``unit,sex,age,geo\\time`` and the
    remaining columns are year values.
    """

    def load_data(self, file_path: Path) -> pd.DataFrame:
        """Load and clean the Eurostat TSV file.

        Args:
            file_path: Path to the ``.tsv`` file.

        Returns:
            Long-format DataFrame with columns
            ``unit``, ``sex``, ``age``, ``region``, ``year``, ``value``.
        """
        df = pd.read_csv(file_path, sep="\t")
        return self._parse_to_long(df)

    def _parse_to_long(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert the wide TSV layout to a tidy long-format DataFrame."""
        # 1. Split the combined first column into four separate columns
        df[["unit", "sex", "age", "region"]] = (
            df.iloc[:, 0].str.split(",", expand=True)
        )
        df = df.drop(df.columns[0], axis=1)

        # 2. Melt from wide to long format
        df = df.melt(
            id_vars=["unit", "sex", "age", "region"],
            var_name="year",
            value_name="value",
        )

        # 3. Clean year column
        df["year"] = df["year"].str.strip().astype(int)

        # 4. Clean value column: remove sentinels and flags, coerce to float
        df["value"] = df["value"].str.strip().replace(":", pd.NA)
        df["value"] = df["value"].str.replace(r"\s*[a-z]+$", "", regex=True)
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.dropna(subset=["value"])

        return df


class JSONLoader(AbstractLoader):  # pylint: disable=too-few-public-methods
    """Strategy that loads life expectancy data from an Eurostat JSON file.

    Supports both plain ``.json`` files and ``.zip`` archives containing a
    single JSON file (the format distributed by Eurostat since 2023).
    The Eurostat JSON layout uses a sparse flat-index in ``value`` and
    dimension metadata in ``dimension``.
    """

    def load_data(self, file_path: Path) -> pd.DataFrame:
        """Load and clean an Eurostat JSON (or zipped JSON) file.

        Args:
            file_path: Path to a ``.json`` or ``.zip`` file.

        Returns:
            Long-format DataFrame with columns
            ``unit``, ``sex``, ``age``, ``region``, ``year``, ``value``.
        """
        data = self._read_json(file_path)
        return self._parse_to_long(data)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _read_json(file_path: Path) -> dict:
        """Read JSON data from a plain file or a zip archive."""
        if file_path.suffix == ".zip":
            with zipfile.ZipFile(file_path) as zf:
                json_name = next(
                    name for name in zf.namelist() if name.endswith(".json")
                )
                with zf.open(json_name) as fh:
                    return json.load(fh)

        with open(file_path, encoding="utf-8") as fh:
            return json.load(fh)

    @staticmethod
    def _build_categories(data: dict) -> dict[str, list[str]]:
        """Return an ordered list of category labels for each dimension."""
        return {
            dim: sorted(
                data["dimension"][dim]["category"]["index"],
                key=lambda k, ci=data["dimension"][dim]["category"]["index"]: ci[k],
            )
            for dim in data["id"]
        }

    @staticmethod
    def _build_strides(sizes: list[int]) -> list[int]:
        """Return row-major strides for the given dimension sizes."""
        strides: list[int] = []
        stride = 1
        for size in reversed(sizes):
            strides.insert(0, stride)
            stride *= size
        return strides

    @staticmethod
    def _decode_row(
        flat_idx: int,
        dimensions: list[str],
        strides: list[int],
        categories: dict[str, list[str]],
    ) -> dict[str, object]:
        """Decode a flat sparse index into a dimension-label mapping."""
        row: dict[str, object] = {}
        remaining = flat_idx
        for dim, dim_stride in zip(dimensions, strides):
            cat_idx = remaining // dim_stride
            remaining = remaining % dim_stride
            row[dim] = categories[dim][cat_idx]
        return row

    @staticmethod
    def _parse_to_long(data: dict) -> pd.DataFrame:
        """Parse the Eurostat JSON structure into a tidy long-format DataFrame.

        The Eurostat API returns a sparse ``value`` dict keyed by the flat
        index over all dimension combinations (row-major order).  This method
        recovers the individual dimension values for each entry.
        """
        dimensions: list[str] = data["id"]
        sizes: list[int] = data["size"]
        values: dict[str, float] = data["value"]

        categories = JSONLoader._build_categories(data)
        strides = JSONLoader._build_strides(sizes)

        rows = [
            {**JSONLoader._decode_row(int(k), dimensions, strides, categories),
             "value": float(v)}
            for k, v in values.items()
        ]

        df = pd.DataFrame(rows)
        df = df.rename(columns={"geo": "region", "time": "year"})
        df["year"] = df["year"].astype(int)
        return df
