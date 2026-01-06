"""Module for cleaning life expectancy data"""
import argparse
from pathlib import Path
import pandas as pd


def load_data(input_file: Path) -> pd.DataFrame:
    """
    Load the raw life expectancy data from a TSV file.

    Args:
        input_file: Path to the input TSV file

    Returns:
        DataFrame with the raw data
    """
    return pd.read_csv(input_file, sep="\t")


def clean_data(df: pd.DataFrame, region: str = "PT") -> pd.DataFrame:
    """
    Clean and process life expectancy data.

    This function unpivots the data to long format, cleans the data types,
    and filters by region.

    Args:
        df: Raw DataFrame from the TSV file
        region: The region code to filter (default: "PT" for Portugal)

    Returns:
        Cleaned DataFrame ready to be saved
    """
    # 1. Split the first column into separate columns
    # The first column format is: unit,sex,age,geo\time
    df[["unit", "sex", "age", "region"]] = df.iloc[:, 0].str.split(",", expand=True)

    # Drop the original combined column
    df = df.drop(df.columns[0], axis=1)

    # 2. Unpivot to long format
    df = df.melt(
        id_vars=["unit", "sex", "age", "region"],
        var_name="year",
        value_name="value"
    )

    # 3. Clean year column - remove extra spaces and convert to int
    df["year"] = df["year"].str.strip().astype(int)

    # 4. Clean value column
    # Replace ":" with NaN, remove spaces and trailing flags (b, e, p, etc.), convert to float
    df["value"] = df["value"].str.strip().replace(":", pd.NA)
    df["value"] = df["value"].str.replace(r"\s*[a-z]+$", "", regex=True)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["value"])

    # 5. Filter by region
    df = df[df["region"] == region]

    return df


def save_data(df: pd.DataFrame, output_file: Path) -> None:
    """
    Save the cleaned data to a CSV file.

    Args:
        df: Cleaned DataFrame to save
        output_file: Path to the output CSV file
    """
    # Create parent directories if they don't exist
    output_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_file, index=False)


def clean_data_pipeline(region: str = "PT") -> pd.DataFrame:
    """
    Execute the complete data cleaning pipeline.

    Args:
        region: The region code to filter (default: "PT" for Portugal)
        
    Returns:
        Cleaned DataFrame
    """
    # Define paths
    data_dir = Path(__file__).parent / "data"
    input_file = data_dir / "eu_life_expectancy_raw.tsv"
    output_file = data_dir / f"{region.lower()}_life_expectancy.csv"

    # Execute the pipeline
    raw_data = load_data(input_file)
    cleaned_data = clean_data(raw_data, region)
    save_data(cleaned_data, output_file)
    
    return cleaned_data


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser(description="Clean life expectancy data")
    parser.add_argument(
        "--country",
        type=str,
        default="PT",
        help="Country code to filter (default: PT)"
    )
    args = parser.parse_args()
    clean_data_pipeline(region=args.country)
