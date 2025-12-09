"""Module for cleaning life expectancy data"""
import argparse
from pathlib import Path
import pandas as pd


def clean_data(region: str = "PT") -> None:
    """
    Clean and process life expectancy data.

    This function loads the raw EU life expectancy data, unpivots it to long format,
    cleans the data types, filters by region, and saves the result.

    Args:
        region: The region code to filter (default: "PT" for Portugal)
    """
    # Define paths
    data_dir = Path(__file__).parent / "data"
    input_file = data_dir / "eu_life_expectancy_raw.tsv"
    output_file = data_dir / "pt_life_expectancy.csv"

    # 1. Load the data
    df = pd.read_csv(input_file, sep="\t")

    # 2. Split the first column into separate columns
    # The first column format is: unit,sex,age,geo\time
    df[["unit", "sex", "age", "region"]] = df.iloc[:, 0].str.split(",", expand=True)

    # Drop the original combined column
    df = df.drop(df.columns[0], axis=1)

    # 3. Unpivot to long format
    df = df.melt(
        id_vars=["unit", "sex", "age", "region"],
        var_name="year",
        value_name="value"
    )

    # 4. Clean year column - remove extra spaces and convert to int
    df["year"] = df["year"].str.strip().astype(int)

    # 5. Clean value column
    # Replace ":" with NaN, remove spaces and trailing 'e', convert to float, and drop NaNs
    df["value"] = df["value"].str.strip().replace(":", pd.NA)
    df["value"] = df["value"].str.replace(r"\s*e$", "", regex=True)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["value"])

    # 6. Filter by region
    df = df[df["region"] == region]

    # 7. Save to CSV without index
    df.to_csv(output_file, index=False)


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser(description="Clean life expectancy data")
    parser.add_argument(
        "--country",
        type=str,
        default="PT",
        help="Country code to filter (default: PT)"
    )
    args = parser.parse_args()
    clean_data(region=args.country)
