"""REST client handling, including TiingoStream base class."""

from __future__ import annotations

import logging
import re
import typing as t
from decimal import Decimal
from uuid import NAMESPACE_DNS, uuid5

import backoff
import requests
from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.helpers.types import Context
from singer_sdk.pagination import BaseAPIPaginator  # noqa: TC002
from singer_sdk.streams import RESTStream


class TiingoStream(RESTStream):
    """Tiingo stream class."""

    records_jsonpath = "$[*]"

    _add_ticker = True
    _add_surrogate_key = False

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config.get("api_url", "https://api.tiingo.com")

    @property
    def authenticator(self) -> APIKeyAuthenticator:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return APIKeyAuthenticator.create_for_stream(
            self,
            key="Authorization",
            value=f"Token {self.config.get('api_key', '')}",
            location="header",
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        # If not using an authenticator, you may also provide inline auth headers:
        # headers["Private-Token"] = self.config.get("auth_token")  # noqa: ERA001
        return {}

    def get_new_paginator(self) -> BaseAPIPaginator:
        """Create a new pagination helper instance.

        If the source API can make use of the `next_page_token_jsonpath`
        attribute, or it contains a `X-Next-Page` header in the response
        then you can remove this method.

        If you need custom pagination that uses page numbers, "next" links, or
        other approaches, please read the guide: https://sdk.meltano.com/en/v0.25.0/guides/pagination-classes.html.

        Returns:
            A pagination helper instance.
        """
        return super().get_new_paginator()

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: t.Any | None,
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        return {}

    def prepare_request_payload(
        self,
        context: Context | None,
        next_page_token: t.Any | None,
    ) -> dict | None:
        """Prepare the data payload for the REST API request.

        Returns:
            None - Tiingo API uses GET requests only.
        """
        return None

    @staticmethod
    def _redact_api_key(msg):
        return re.sub(r"(api_key=)([^\s&]+)", r"\1<REDACTED>", msg)

    @staticmethod
    def _to_snake_case(camel_str: str) -> str:
        """Convert camelCase to snake_case."""
        s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", camel_str)
        return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()

    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException,),
        base=5,
        max_value=300,
        jitter=backoff.full_jitter,
        max_tries=12,
        max_time=1800,
        giveup=lambda e: (
            isinstance(e, requests.exceptions.HTTPError)
            and e.response is not None
            and e.response.status_code not in (429, 500, 502, 503, 504)
        ),
        on_backoff=lambda details: logging.warning(
            f"API request failed, retrying in {details['wait']:.1f}s " f"(attempt {details['tries']}): {details['exception']}"
        ),
    )
    def fetch_with_retry(self, url: str, params: dict = None) -> requests.Response:
        """Fetch data with exponential backoff retry logic."""
        # Log request details with API key redaction
        log_url = self._redact_api_key(url)
        log_params = {k: ("<REDACTED>" if "api" in k.lower() else v) for k, v in (params or {}).items()}
        self.logger.info(f"Stream {self.name}: Requesting {log_url} with params: {log_params}")

        response = self.requests_session.get(
            url,
            params=params,
            headers=self.http_headers,
            auth=self.authenticator,
            timeout=self.timeout,
        )
        response.raise_for_status()

        # Log successful response
        self.logger.info(f"Stream {self.name}: Response {response.status_code} received")
        return response

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Request records from the API using retry logic."""
        next_page_token = None

        while True:
            url = self.get_url(context)
            params = self.get_url_params(context, next_page_token)

            response = self.fetch_with_retry(url, params)

            for record in self.parse_response(response):
                yield record

            paginator = self.get_new_paginator()
            if hasattr(paginator, "get_next") and paginator.get_next:
                next_page_token = paginator.get_next(response)
                if not next_page_token:
                    break
            else:
                break

    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        try:
            records = response.json(parse_float=Decimal)
            if isinstance(records, list):
                records_snake_case = [{self._to_snake_case(k): v for k, v in record.items()} for record in records]
            elif isinstance(records, dict):
                records_snake_case = {self._to_snake_case(k): v for k, v in records.items()}
            yield from extract_jsonpath(
                self.records_jsonpath,
                input=records_snake_case,
            )
        except Exception as e:
            error_msg = f"Error parsing response: {e}"
            self.logger.error(self._redact_api_key(error_msg))
            raise e

    @staticmethod
    def _generate_surrogate_key(data: dict, namespace=NAMESPACE_DNS) -> str:
        key_values = [str(data.get(field, "")) for field in data]
        key_string = "|".join(key_values)
        return str(uuid5(namespace, key_string))

    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        """Post-process record data.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record dictionary.
        """
        if not context or "ticker" not in context:
            raise ValueError("ticker context is required")

        if self._add_ticker:
            row["ticker"] = context["ticker"]
        if self._add_surrogate_key:
            row["surrogate_key"] = self._generate_surrogate_key(row)
        return row
