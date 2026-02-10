"""Module for region enumeration"""
from enum import Enum


class Region(Enum):
    """Enumeration of all available regions in the life expectancy dataset"""

    # Individual countries
    AL = "AL"  # Albania
    AM = "AM"  # Armenia
    AT = "AT"  # Austria
    AZ = "AZ"  # Azerbaijan
    BE = "BE"  # Belgium
    BG = "BG"  # Bulgaria
    BY = "BY"  # Belarus
    CH = "CH"  # Switzerland
    CY = "CY"  # Cyprus
    CZ = "CZ"  # Czech Republic
    DE = "DE"  # Germany
    DK = "DK"  # Denmark
    EE = "EE"  # Estonia
    EL = "EL"  # Greece
    ES = "ES"  # Spain
    FI = "FI"  # Finland
    FR = "FR"  # France
    GE = "GE"  # Georgia
    HR = "HR"  # Croatia
    HU = "HU"  # Hungary
    IE = "IE"  # Ireland
    IS = "IS"  # Iceland
    IT = "IT"  # Italy
    LI = "LI"  # Liechtenstein
    LT = "LT"  # Lithuania
    LU = "LU"  # Luxembourg
    LV = "LV"  # Latvia
    MD = "MD"  # Moldova
    ME = "ME"  # Montenegro
    MK = "MK"  # North Macedonia
    MT = "MT"  # Malta
    NL = "NL"  # Netherlands
    NO = "NO"  # Norway
    PL = "PL"  # Poland
    PT = "PT"  # Portugal
    RO = "RO"  # Romania
    RS = "RS"  # Serbia
    RU = "RU"  # Russia
    SE = "SE"  # Sweden
    SI = "SI"  # Slovenia
    SK = "SK"  # Slovakia
    SM = "SM"  # San Marino
    TR = "TR"  # Turkey
    UA = "UA"  # Ukraine
    UK = "UK"  # United Kingdom
    XK = "XK"  # Kosovo

    # Aggregate regions (not actual countries)
    DE_TOT = "DE_TOT"  # Germany Total
    EA18 = "EA18"  # Euro Area 18
    EA19 = "EA19"  # Euro Area 19
    EEA30_2007 = "EEA30_2007"  # European Economic Area 30 (2007)
    EEA31 = "EEA31"  # European Economic Area 31
    EFTA = "EFTA"  # European Free Trade Association
    EU27_2007 = "EU27_2007"  # European Union 27 (2007)
    EU27_2020 = "EU27_2020"  # European Union 27 (2020)
    EU28 = "EU28"  # European Union 28
    FX = "FX"  # France Metropolitan

    @classmethod
    def countries(cls) -> list['Region']:
        """
        Return a list of all actual countries (excluding aggregate regions).
        
        Returns:
            List of Region enum members representing actual countries
        """
        # Aggregate regions to exclude
        aggregates = {
            cls.DE_TOT,
            cls.EA18,
            cls.EA19,
            cls.EEA30_2007,
            cls.EEA31,
            cls.EFTA,
            cls.EU27_2007,
            cls.EU27_2020,
            cls.EU28,
            cls.FX
        }

        return [region for region in cls if region not in aggregates]
