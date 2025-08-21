"""Stream type classes for tap-tiingo."""

from __future__ import annotations

import typing as t

from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_tiingo.client import TiingoStream


class BaseTickerPartitionStream(TiingoStream):
    """Base stream for ticker-partitioned data."""

    @property
    def partitions(self) -> list[dict]:
        """Return a list of partition contexts."""
        raise NotImplementedError("Subclasses must implement partitions property")

    def get_starting_replication_key_value(self, context: Context | None) -> t.Any | None:
        """Get starting replication key value from state or config."""
        if not self.replication_key:
            return None

        state = self.get_context_state(context)
        if state and "replication_key_value" in state:
            return state["replication_key_value"]

        return self.config.get("start_date")

    def get_url_params(self, context: Context | None, next_page_token: t.Any | None) -> dict[str, t.Any]:
        """Return URL parameters with incremental replication support."""
        params = {}

        if self.replication_key:
            start_value = self.get_starting_replication_key_value(context)
            if start_value:
                params["startDate"] = start_value

        if self.config.get("end_date"):
            params["endDate"] = self.config["end_date"]

        return params


class StockTickerPartitionStream(BaseTickerPartitionStream):
    """Stock ticker partition stream."""

    @property
    def partitions(self) -> list[dict]:
        """Return a list of stock ticker partition contexts."""
        return [{"ticker": t["ticker"]} for t in self._tap.get_cached_stock_tickers()]


class TickerMetadataStream(StockTickerPartitionStream):
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


class DailyPricesStream(StockTickerPartitionStream):
    """Tiingo daily prices stream."""

    name = "daily_prices"
    path = "/tiingo/daily"
    primary_keys: t.ClassVar[list[str]] = ["ticker", "date"]
    replication_key = "date"
    replication_method = "INCREMENTAL"
    records_jsonpath = "$[*]"

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
