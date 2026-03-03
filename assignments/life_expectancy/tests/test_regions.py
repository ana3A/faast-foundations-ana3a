"""Tests for the regions module"""
import pytest

from life_expectancy.regions import Region


def test_region_enum_has_all_regions():
    """Test that Region enum contains all expected regions"""
    # Test a sample of individual countries
    assert Region.PT.value == "PT"
    assert Region.FR.value == "FR"
    assert Region.DE.value == "DE"
    assert Region.ES.value == "ES"
    
    # Test aggregate regions
    assert Region.EU27_2020.value == "EU27_2020"
    assert Region.EU28.value == "EU28"
    assert Region.EFTA.value == "EFTA"


def test_region_countries_excludes_aggregates():
    """Test that countries() method excludes aggregate regions"""
    countries = Region.countries()
    
    # Aggregate regions that should be excluded
    aggregates = [
        Region.DE_TOT,
        Region.EA18,
        Region.EA19,
        Region.EEA30_2007,
        Region.EEA31,
        Region.EFTA,
        Region.EU27_2007,
        Region.EU27_2020,
        Region.EU28,
        Region.FX
    ]
    
    # Check that no aggregate is in the countries list
    for aggregate in aggregates:
        assert aggregate not in countries


def test_region_countries_includes_actual_countries():
    """Test that countries() method includes actual countries"""
    countries = Region.countries()
    
    # Sample of actual countries that should be included
    expected_countries = [
        Region.PT,
        Region.FR,
        Region.DE,
        Region.ES,
        Region.IT,
        Region.UK,
        Region.PL,
        Region.RO,
        Region.NL,
        Region.BE
    ]
    
    # Check that all expected countries are in the list
    for country in expected_countries:
        assert country in countries


def test_region_countries_returns_list():
    """Test that countries() method returns a list"""
    countries = Region.countries()
    assert isinstance(countries, list)
    assert len(countries) > 0


def test_region_countries_correct_count():
    """Test that countries() method returns the correct number of countries"""
    countries = Region.countries()
    
    # Total regions in enum
    total_regions = len(Region)
    
    # Number of aggregate regions
    num_aggregates = 10  # DE_TOT, EA18, EA19, EEA30_2007, EEA31, EFTA, EU27_2007, EU27_2020, EU28, FX
    
    # Countries should be total minus aggregates
    expected_count = total_regions - num_aggregates
    
    assert len(countries) == expected_count


def test_region_enum_membership():
    """Test that we can access regions via enum membership"""
    # Test accessing by name
    assert Region["PT"] == Region.PT
    assert Region["FR"] == Region.FR
    
    # Test accessing by value
    assert Region.PT.value == "PT"
    assert Region.FR.value == "FR"


def test_region_enum_iteration():
    """Test that we can iterate over all regions"""
    regions = list(Region)
    
    # Check that we get all regions
    assert len(regions) > 0
    
    # Check that PT and FR are in the list
    assert Region.PT in regions
    assert Region.FR in regions
    assert Region.EU28 in regions
