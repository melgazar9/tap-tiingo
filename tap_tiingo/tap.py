"""Tiingo tap class."""

from __future__ import annotations
import threading
from typing import List, Dict, Any

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_tiingo.client import TiingoStream

from tap_tiingo.streams import (
    TickerMetadataStream,
    DailyPricesStream
)


class TapTiingo(Tap):
    """Tiingo tap class."""

    name = "tap-tiingo"
    _cached_tickers: List[Dict[str, Any]] = None
    _ticker_lock = threading.Lock()

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType(nullable=False),
            required=True,
            secret=True,
            title="API Key",
            description="The API key to authenticate against the Tiingo API service",
        ),
        th.Property(
            "tickers",
            th.OneOf(
                th.StringType,
                th.ArrayType(th.StringType(nullable=False), nullable=False),
            ),
            required=True,
            title="Stock Tickers",
            description="List of stock tickers to replicate (e.g., ['AAPL', 'GOOGL']) or '*' for all available tickers.",
        ),
        th.Property(
            "start_date",
            th.DateTimeType(nullable=True),
            description="The earliest record date to sync",
        ),
        th.Property(
            "api_url",
            th.StringType(nullable=False),
            title="API URL",
            default="https://api.tiingo.com",
            description="The base URL for the Tiingo API service",
        ),
        th.Property(
            "user_agent",
            th.StringType(nullable=True),
            description=(
                "A custom User-Agent header to send with each request. Default is "
                "'<tap_name>/<tap_version>'"
            ),
        ),
    ).to_dict()

    def get_cached_tickers(self) -> List[Dict[str, Any]]:
        """Get cached tickers with thread safety."""
        if self._cached_tickers is not None:
            return self._cached_tickers

        with self._ticker_lock:
            if self._cached_tickers is not None:
                return self._cached_tickers

            tickers_config = self.config["tickers"]
            
            if tickers_config == "*":
                # TODO: Implement fetching all available tickers from Tiingo API
                # For now, use a default set
                self._cached_tickers = [
                    {"ticker": "AAPL"},
                    {"ticker": "GOOGL"},
                    {"ticker": "MSFT"},
                    {"ticker": "TSLA"},
                ]
            elif isinstance(tickers_config, list):
                self._cached_tickers = [{"ticker": ticker} for ticker in tickers_config]
            else:
                raise ValueError("tickers must be '*' or a list of ticker strings")

        return self._cached_tickers

    def discover_streams(self) -> list[TiingoStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            TickerMetadataStream(self),
            DailyPricesStream(self),
        ]


if __name__ == "__main__":
    TapTiingo.cli()
