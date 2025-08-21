"""Stream type classes for tap-tiingo."""

from __future__ import annotations

import typing as t

from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_tiingo.client import TiingoStream


class TickerPartitionStream(TiingoStream):
    """Base stream for ticker-partitioned data."""

    @property
    def partitions(self) -> list[dict]:
        """Return a list of partition contexts."""
        return [{"ticker": t["ticker"]} for t in self._tap.get_cached_tickers()]

    def post_process(self, row: dict, context: Context | None = None) -> dict | None:
        """Post-process record to transform camelCase to snake_case."""
        if not context or "ticker" not in context:
            raise ValueError("ticker context is required")

        row["ticker"] = context["ticker"]

        transformed = {}
        for key, value in row.items():
            new_key = self._get_field_mapping().get(key, key)
            transformed[new_key] = value
        
        return transformed

    def _get_field_mapping(self) -> dict[str, str]:
        """Get field mapping for camelCase to snake_case transformation."""
        return {}


class TickerMetadataStream(TickerPartitionStream):
    """Tiingo ticker metadata stream."""

    name = "ticker_metadata"
    path = "/tiingo/daily"
    primary_keys: t.ClassVar[list[str]] = ["ticker"]
    replication_key = None
    records_jsonpath = "$"  # Single object response

    def get_url(self, context: Context | None) -> str:
        if not context or "ticker" not in context:
            raise ValueError("'ticker' not found in context.")

        return f"{self.url_base}/tiingo/daily/{context['ticker']}"

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType, description="Stock ticker symbol"),
        th.Property("name", th.StringType, description="Company name"),
        th.Property("description", th.StringType, description="Company description"),
        th.Property("start_date", th.DateType, description="Start date of data availability"),
        th.Property("end_date", th.DateType, description="End date of data availability"),
        th.Property("exchange_code", th.StringType, description="Exchange code"),
    ).to_dict()

    def _get_field_mapping(self) -> dict[str, str]:
        """Get field mapping for camelCase to snake_case transformation."""
        return {
            "startDate": "start_date",
            "endDate": "end_date",
            "exchangeCode": "exchange_code",
        }


class DailyPricesStream(TickerPartitionStream):
    """Tiingo daily prices stream."""

    name = "daily_prices"
    path = "/tiingo/daily"
    primary_keys: t.ClassVar[list[str]] = ["ticker", "date"]
    replication_key = "date"
    records_jsonpath = "$[*]"  # Array of price records

    def get_url(self, context: Context | None) -> str:
        """Get stream entity URL."""
        if not context or "ticker" not in context:
            raise ValueError("ticker context is required")
        
        ticker = context["ticker"]
        return f"{self.url_base}/tiingo/daily/{ticker}/prices"

    schema = th.PropertiesList(
        th.Property("ticker", th.StringType, description="Stock ticker symbol"),
        th.Property("date", th.DateType, description="Date of the price data"),
        th.Property("close", th.NumberType, description="Closing price"),
        th.Property("high", th.NumberType, description="High price"),
        th.Property("low", th.NumberType, description="Low price"),
        th.Property("open", th.NumberType, description="Opening price"),
        th.Property("volume", th.IntegerType, description="Trading volume"),
        th.Property("adj_close", th.NumberType, description="Adjusted closing price"),
        th.Property("adj_high", th.NumberType, description="Adjusted high price"),
        th.Property("adj_low", th.NumberType, description="Adjusted low price"),
        th.Property("adj_open", th.NumberType, description="Adjusted opening price"),
        th.Property("adj_volume", th.IntegerType, description="Adjusted trading volume"),
        th.Property("div_cash", th.NumberType, description="Dividend cash amount"),
        th.Property("split_factor", th.NumberType, description="Stock split factor"),
    ).to_dict()

    def _get_field_mapping(self) -> dict[str, str]:
        """Get field mapping for camelCase to snake_case transformation."""
        return {
            "adjClose": "adj_close",
            "adjHigh": "adj_high",
            "adjLow": "adj_low",
            "adjOpen": "adj_open",
            "adjVolume": "adj_volume",
            "divCash": "div_cash",
            "splitFactor": "split_factor",
        }

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any]:
        """Return URL parameters."""
        params = {}
        if self.config.get("start_date"):
            params["startDate"] = self.config["start_date"]
        return params
