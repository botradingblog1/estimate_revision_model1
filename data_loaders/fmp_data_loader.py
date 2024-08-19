import os
import requests
import pandas as pd
from enum import Enum
from typing import Union


class Period(Enum):
    QUARTERLY = "quarter"
    ANNUAL = "annual"



class FmpDataLoader:
    """
    FmpDataLoader provides methods to interact with the Financial Modeling Prep (FMP) API to fetch various financial data.

    Attributes:
        api_key (str): FMP API key.
    """

    def __init__(self, api_key: str):
        """
        Initializes the FmpDataLoader with the given API key.

        Parameters:
            api_key (str): FMP API key.
        """
        self._api_key = api_key

    def fetch_stock_screener_results(
        self,
        exchange_list=None,
        market_cap_more_than=None,
        market_cap_lower_than=None,
        price_more_than=None,
        price_lower_than=None,
        beta_more_than=None,
        beta_lower_than=None,
        volume_more_than=None,
        volume_lower_than=None,
        dividend_more_than=None,
        dividend_lower_than=None,
        is_etf=None,
        is_fund=None,
        is_actively_trading=None,
        sector=None,
        industry=None,
        country=None,
        exchange=None,
        limit=1000,
        cache_data=False,
        cache_dir="cache",
        file_name="eft_data.csv"
    ):
        """
        Fetches stock screener results from the FMP API.

        Parameters:
            exchange_list (str): List of exchanges.
            market_cap_more_than (int): Minimum market cap.
            market_cap_lower_than (int): Maximum market cap.
            price_more_than (float): Minimum stock price.
            price_lower_than (float): Maximum stock price.
            beta_more_than (float): Minimum beta.
            beta_lower_than (float): Maximum beta.
            volume_more_than (int): Minimum volume.
            volume_lower_than (int): Maximum volume.
            dividend_more_than (float): Minimum dividend yield.
            dividend_lower_than (float): Maximum dividend yield.
            is_etf (bool): Filter for ETFs.
            is_fund (bool): Filter for funds.
            is_actively_trading (bool): Filter for actively trading stocks.
            sector (str): Sector filter.
            industry (str): Industry filter.
            country (str): Country filter.
            exchange (str): Exchange filter.
            limit (int): Maximum number of results.
            cache_data (bool): Cache data locally?
            cache_dir (str): cache directory
            file_name (str): cache file name

        Returns:
            pd.DataFrame: DataFrame with stock screener results.
        """
        try:
            path = os.path.join(cache_dir, file_name)

            # Try to load from cache
            if cache_data and os.path.exists(path):
                securities_df = pd.read_csv(path)
                return securities_df

            # Load data remotely
            url = "https://financialmodelingprep.com/api/v3/stock-screener?"
            params = {
                "exchange": exchange_list,
                "limit": limit,
                "marketCapMoreThan": market_cap_more_than,
                "marketCapLowerThan": market_cap_lower_than,
                "priceMoreThan": price_more_than,
                "priceLowerThan": price_lower_than,
                "betaMoreThan": beta_more_than,
                "betaLowerThan": beta_lower_than,
                "volumeMoreThan": volume_more_than,
                "volumeLowerThan": volume_lower_than,
                "dividendMoreThan": dividend_more_than,
                "dividendLowerThan": dividend_lower_than,
                "isEtf": is_etf,
                "isFund": is_fund,
                "isActivelyTrading": is_actively_trading,
                "sector": sector,
                "industry": industry,
                "country": country,
                "exchange": exchange,
                "apikey": self._api_key
            }

            # Filter out parameters that are None
            params = {k: v for k, v in params.items() if v is not None}

            response = requests.get(url, params=params)

            if response.status_code == 200:
                securities_data = response.json()
                if securities_data:
                    securities_df = pd.DataFrame(securities_data)

                    # Cache locally if requested
                    if cache_data:
                        os.makedirs(cache_dir, exist_ok=True)
                        securities_df.to_csv(path, index=False)

                    return securities_df
                return None
            else:
                return None
        except Exception as ex:
            print(ex)
            return None

    def fetch_analyst_estimates(self, symbol: str, period: Period, limit: int) -> Union[pd.DataFrame, None]:
        """
        Fetches analyst estimates data from the FMP API.

        Parameters:
            symbol (str): Stock symbol.
            period (Period): Period for the estimates, either Period.QUARTERLY or Period.ANNUAL.
            limit (int): Number of records to fetch.
            api_key (str): Your FMP API key.

        Returns:
            pd.DataFrame: DataFrame with analyst estimates data or None if the request fails.
        """
        try:
            url = f"https://financialmodelingprep.com/api/v3/analyst-estimates/{symbol}?period={period.value}&limit={limit}&apikey={self._api_key}"
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                if data:
                    estimates_df = pd.DataFrame(data)
                    return estimates_df
                else:
                    print(f"No data found for {symbol}.")
                    return None
            else:
                print(f"Failed to fetch analyst estimates. Error: {response.reason}")
                return None
        except Exception as ex:
            print(ex)
            return None

    def fetch_earnings_surprises(self, symbol: str) -> Union[pd.DataFrame, None]:
        """
        Fetches earnings surprises data from the FMP API.

        Parameters:
            symbol (str): Stock symbol.

        Returns:
            pd.DataFrame: DataFrame with earnings surprises data or None if the request fails.
        """
        try:
            url = f"https://financialmodelingprep.com/api/v3/earnings-surprises/{symbol}?apikey={self._api_key}"
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                if data:
                    surprises_df = pd.DataFrame(data)
                    return surprises_df
                else:
                    print(f"No data found for {symbol}.")
                    return None
            else:
                print(f"Failed to fetch earnings surprises. Error: {response.reason}")
                return None
        except Exception as ex:
            print(ex)
            return None
