"""Tests for the cleaning module"""
import tempfile
from pathlib import Path
import pandas as pd
import pytest

from life_expectancy.cleaning import (
    load_data,
    clean_data,
    save_data,
    clean_data_pipeline
)
from . import OUTPUT_DIR, PACKAGE_DIR


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


# Unit tests for individual functions

def test_load_data():
    """Test that load_data correctly loads the TSV file"""
    input_file = PACKAGE_DIR / "data" / "eu_life_expectancy_raw.tsv"
    df = load_data(input_file)
    
    # Check that data was loaded
    assert not df.empty
    assert isinstance(df, pd.DataFrame)
    
    # Check that first column contains combined data
    first_col_name = df.columns[0]
    assert "," in df[first_col_name].iloc[0]
    
    # Check that year columns exist (with potential spaces)
    year_cols = [col for col in df.columns if col.strip().isdigit()]
    assert len(year_cols) > 0


def test_clean_data_structure():
    """Test that clean_data produces the correct structure"""
    # Create a small sample DataFrame that mimics raw data
    raw_df = pd.DataFrame({
        "unit,sex,age,geo\\time": ["YR,F,Y65,PT", "YR,M,Y65,PT"],
        "2020 ": ["21.5 e", "18.2 e"],
        "2021 ": ["21.7", "18.4"]
    })
    
    cleaned = clean_data(raw_df, region="PT")
    
    # Check expected columns
    expected_cols = ["unit", "sex", "age", "region", "year", "value"]
    assert list(cleaned.columns) == expected_cols
    
    # Check data types
    assert cleaned["year"].dtype in ["int64", "int32"]
    assert cleaned["value"].dtype == "float64"
    
    # Check that we have the expected number of rows (2 rows × 2 years = 4 rows)
    assert len(cleaned) == 4
    
    # Check that region filter worked
    assert all(cleaned["region"] == "PT")


def test_clean_data_removes_flags():
    """Test that clean_data removes all data quality flags (b, e, p, etc.)"""
    raw_df = pd.DataFrame({
        "unit,sex,age,geo\\time": ["YR,F,Y65,FR", "YR,F,Y65,FR", "YR,F,Y65,FR", "YR,F,Y65,FR"],
        "2020": ["21.5 e", "21.6 b", "21.7 p", "21.8 bep"]
    })
    
    cleaned = clean_data(raw_df, region="FR")
    
    # Check that values are clean floats
    expected_values = [21.5, 21.6, 21.7, 21.8]
    assert cleaned["value"].tolist() == expected_values


def test_clean_data_filters_region():
    """Test that clean_data correctly filters by region"""
    raw_df = pd.DataFrame({
        "unit,sex,age,geo\\time": ["YR,F,Y65,PT", "YR,F,Y65,FR", "YR,F,Y65,DE"],
        "2020": ["21.5", "23.4", "20.8"]
    })
    
    cleaned = clean_data(raw_df, region="FR")
    
    # Should only have French data
    assert len(cleaned) == 1
    assert all(cleaned["region"] == "FR")
    assert cleaned["value"].iloc[0] == 23.4


def test_clean_data_handles_missing_values():
    """Test that clean_data removes missing values marked with ':'"""
    raw_df = pd.DataFrame({
        "unit,sex,age,geo\\time": ["YR,F,Y65,PT", "YR,M,Y65,PT", "YR,T,Y65,PT"],
        "2020": ["21.5", ":", "20.0"]
    })
    
    cleaned = clean_data(raw_df, region="PT")
    
    # Should only have 2 rows (the ':' should be filtered out)
    assert len(cleaned) == 2
    assert 21.5 in cleaned["value"].values
    assert 20.0 in cleaned["value"].values


def test_save_data():
    """Test that save_data correctly writes a CSV file"""
    # Create a sample DataFrame
    df = pd.DataFrame({
        "unit": ["YR", "YR"],
        "sex": ["F", "M"],
        "age": ["Y65", "Y65"],
        "region": ["PT", "PT"],
        "year": [2020, 2020],
        "value": [21.5, 18.2]
    })
    
    # Use a temporary file
    with tempfile.TemporaryDirectory() as tmpdir:
        output_file = Path(tmpdir) / "test_output.csv"
        save_data(df, output_file)
        
        # Check that file was created
        assert output_file.exists()
        
        # Read it back and verify
        loaded_df = pd.read_csv(output_file)
        pd.testing.assert_frame_equal(df, loaded_df)


def test_save_data_creates_directories():
    """Test that save_data creates parent directories if they don't exist"""
    df = pd.DataFrame({
        "unit": ["YR"],
        "sex": ["F"],
        "age": ["Y65"],
        "region": ["PT"],
        "year": [2020],
        "value": [21.5]
    })
    
    # Use a temporary directory with nested path
    with tempfile.TemporaryDirectory() as tmpdir:
        output_file = Path(tmpdir) / "nested" / "dirs" / "test_output.csv"
        
        # Directory should not exist yet
        assert not output_file.parent.exists()
        
        # save_data should create it
        save_data(df, output_file)
        
        # Check that file and directories were created
        assert output_file.exists()
        assert output_file.parent.exists()
        
        # Verify content
        loaded_df = pd.read_csv(output_file)
        pd.testing.assert_frame_equal(df, loaded_df)


def test_save_data_no_index():
    """Test that save_data doesn't write the DataFrame index"""
    df = pd.DataFrame({
        "unit": ["YR"],
        "value": [21.5]
    })
    
    with tempfile.TemporaryDirectory() as tmpdir:
        output_file = Path(tmpdir) / "test_output.csv"
        save_data(df, output_file)
        
        # Read the file as text and check it doesn't start with index numbers
        with open(output_file, 'r', encoding='utf-8') as f:
            first_line = f.readline().strip()
            # First line should be column names, not numbers
            assert first_line.startswith("unit")
            assert not first_line[0].isdigit()
