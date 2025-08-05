# Prolific-Test

Code and documentation for the Prolific Data Engineer assessment. The code queries the Polygon API using Polygons python client. It currently only queries the [daily OHLC bars](https://polygon.io/docs/rest/stocks/aggregates/custom-bars) for a set of tickers over a defined date range. The ticker, date range and timespan are specified as pipeline arguments. This can be done through the command line or through environment variables.

## Getting Started

The base of the pipeline is written with Polars as the dataframe engine for manipulation. To install the requirements use the following:
```bash
$ pip install -r requirements.txt
```

An API key can be generated using a free tiered account through [Polygon](https://polygon.io). Then define the environment variable for the pipeline:
```bash
$ export POLYGON_API_KEY=<your-api-key>
```

## Usage

The Pipeline is designed to take in a set of stock tickers and return JSON files corresponding to the daily '2025-01-01' to '2025-03-31' date range. It will output a single file per ticker per run. If you re-run the script for a set of tickers it will create a new file tagged with the attempt number. The files are stored in the [data directory](./data/) with subdirectories for each stock ticker.


To use the script run the following in your terminal after setting up your python environment:

```bash
$ python main.py --tickers '["AAPL, "MSFT", "GOOGL"]'
```


## Extensions

There are a couple of extensions and further work that  want to highlight as opportunities for this pipeline.
- Set up the script to run on a schedule to batch ingest historical data. Updating the date range arguments to take the timespan, range and multiplier as arguments to ingest the historical data at different ranges and timespans for additional granularity (if needed).
- Containerise the script using docker.
- Store the data in a database/columnar format, defining a unique id for each timespan and stock.
- Export job metadata to Prometheus or similar monitoring solution.
- Define more data quality checks.
    - Set up data anomaly checks on the stock open, close, high and low prices to validate the quality of the data.
    - Validate that the dates in the data are all weekdays/within trading hours.
