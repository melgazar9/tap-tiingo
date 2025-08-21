# tap-tiingo

`tap-tiingo` is a Singer tap for Tiingo, built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

This tap extracts stock market data from the [Tiingo API](https://www.tiingo.com/), including stock prices and ticker metadata.

## Installation

Install from GitHub:

```bash
pipx install git+https://github.com/melgazar9/tap-tiingo.git@main
```

## Configuration

### Accepted Config Options

| Setting | Required | Default | Description |
|---------|----------|---------|-------------|
| api_key | True | None | The API key to authenticate against the Tiingo API service |
| symbols | False | ["AAPL", "GOOGL"] | List of stock symbols to replicate (e.g., ['AAPL', 'GOOGL']) |
| start_date | False | None | The earliest record date to sync |
| api_url | False | "https://api.tiingo.com" | The base URL for the Tiingo API service |
| user_agent | False | None | A custom User-Agent header to send with each request |

A full list of supported settings and capabilities is available by running:

```bash
tap-tiingo --about
```

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

You need a Tiingo API key to use this tap. Sign up for a free account at [Tiingo](https://www.tiingo.com/) to get your API key.

Set your API key as an environment variable:
```bash
export TIINGO_API_KEY="your_api_key_here"
```

Or include it in your config file:
```json
{
  "api_key": "your_api_key_here",
  "symbols": ["AAPL", "GOOGL", "MSFT"],
  "start_date": "2023-01-01T00:00:00Z"
}
```

## Streams

This tap currently supports two streams:

### ticker_metadata
- **Description**: Stock ticker metadata including company name, description, and data availability dates
- **Primary Keys**: ticker
- **Replication Method**: FULL_TABLE
- **API Endpoint**: `/tiingo/daily/{symbol}`

### daily_prices  
- **Description**: Daily stock price data including OHLCV, adjusted prices, dividends, and split factors
- **Primary Keys**: ticker, date
- **Replication Method**: INCREMENTAL (based on date)
- **API Endpoint**: `/tiingo/daily/{symbol}/prices`

## Usage

You can easily run `tap-tiingo` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-tiingo --version
tap-tiingo --help
tap-tiingo --config CONFIG --discover > ./catalog.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

Prerequisites:

- Python 3.9+
- [uv](https://docs.astral.sh/uv/)

```bash
uv sync
```

### Create and Run Tests

Create tests within the `tests` subfolder and
then run:

```bash
uv run pytest
```

You can also test the `tap-tiingo` CLI interface directly using `uv run`:

```bash
uv run tap-tiingo --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

<!--
Developer TODO:
Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any "TODO" items listed in
the file.
-->

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-tiingo
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-tiingo --version

# OR run a test ELT pipeline:
meltano run tap-tiingo target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
