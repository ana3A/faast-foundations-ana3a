"""Tests for the cleaning module"""
import pandas as pd

from life_expectancy.cleaning import clean_data_pipeline
from . import OUTPUT_DIR


def test_clean_data(pt_life_expectancy_expected):
    """Run the `clean_data_pipeline` function and compare the output to the expected output"""
    clean_data_pipeline()
    pt_life_expectancy_actual = pd.read_csv(
        OUTPUT_DIR / "pt_life_expectancy.csv"
    )
    pd.testing.assert_frame_equal(
        pt_life_expectancy_actual, pt_life_expectancy_expected
    )


def test_clean_data_with_custom_region(eu_life_expectancy_expected):
    """Test clean_data_pipeline with a non-default region (France)"""
    region = "FR"
    output_file = OUTPUT_DIR / f"{region.lower()}_life_expectancy.csv"
    
    # Run pipeline with custom region
    clean_data_pipeline(region=region)
    
    # Load the actual output
    actual_data = pd.read_csv(output_file)
    
    # Filter expected data for the region
    expected_data = eu_life_expectancy_expected[
        eu_life_expectancy_expected["region"] == region
    ].reset_index(drop=True)
    
    # Compare
    pd.testing.assert_frame_equal(actual_data, expected_data)
    
    # Clean up
    output_file.unlink(missing_ok=True)
