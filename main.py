import argparse
import json
import logging
import os
import time

import pandera.polars as pa
import polars as pl

from polygon import RESTClient

class Schema(pa.DataFrameModel):
    open: pl.Float64 = pa.Field(nullable=False)
    high: pl.Float64 = pa.Field(nullable=False)
    low: pl.Float64 = pa.Field(nullable=False)
    close: pl.Float64 = pa.Field(nullable=False)
    volume: pl.Float64 = pa.Field(nullable=False)
    vwap: pl.Float64 = pa.Field(nullable=False)
    timestamp: pl.Datetime(time_unit='ms', time_zone="UTC") = pa.Field(nullable=False)
    transactions: pl.Int64 = pa.Field(nullable=False)
    otc: pl.Boolean = pa.Field(nullable=True)
    market_phase: pl.String = pa.Field(nullable=False, isin=["Neutral", "Bull", "Bear"])


def query_aggregates(client: RESTClient, **kwargs) -> list:
    """
    Use the python client to fetch the aggregate data. Using the internal backoff strategies and factor of 0.1s for retries
    """
    logging.info("Pulling data from aggregate API")
    agg = []
    for a in client.list_aggs(**kwargs):
        agg.append(a)
    return agg


def transformation(data: pl.DataFrame) -> pl.DataFrame:
    """
    Run the following 3 Transformations: 
        1. Convert ms timestamps to Datetime objects in ET timezone.
        2. Convert the timezone from ET to UTC.
        3. Tag each day with a market phase.
    """
    logging.info(f"Beginning Transformation of data set with columns: {data.columns} of length: {data.height}")
    return (
        data
        .with_columns(
            pl.from_epoch(pl.col("timestamp"), time_unit="ms")
            .dt.replace_time_zone("America/New_York")
            .dt.convert_time_zone("UTC")
        )
        .with_columns(
            pl.col("volume").shift(1).alias("volume_1")
        )
        .with_columns(
            pl.when((pl.col("close") > pl.col("open")) & (pl.col("volume") > pl.col("volume_1")))
            .then(pl.lit("Bull"))
            .when((pl.col("close") < pl.col("open")) & (pl.col("volume") > pl.col("volume_1")))
            .then(pl.lit("Bear"))
            .otherwise(pl.lit("Neutral"))
            .alias("market_phase")
        )
        .drop("volume_1")
        .pipe(Schema.validate) # Validate the final transformed output.
    )


def write_json(result_data: pl.DataFrame, start_time, attempt: int, file_path: str):
    result_json = {
        "Execution_Time": time.time() - start_time,
        "Execution_Timestamp": start_time,
        "Attempt": attempt,
        "Records_Processed": result_data.height,
        "Status": "Success",
        "Data": result_data.write_json(),
    }
    logging.info("Writing result to new json file")
    with open(file_path, "w") as f:
        json.dump(result_json, f)

def main(args, start_time):
    logging.info("Generating Polygon client")
    client = RESTClient(args.APIkey, retries=10) # 10 retries with an backoff factor of 0.1
    for stock in json.loads(args.tickers):
        logging.info(f"Collecting data for stock: {stock}")
        data = query_aggregates(
                client=client,
                ticker=stock, 
                multiplier=1,
                timespan="day",
                from_="2025-01-01",
                to="2025-03-31"
        )
        transformed_data = transformation(pl.DataFrame(data))
        if os.path.exists(f"data/{stock}/asofdate=2025-03-31/"):
            attempt = len(os.listdir(f"data/{stock}/asofdate=2025-03-31/")) + 1
        else:
            attempt = 1
            os.makedirs(f"data/{stock}/asofdate=2025-03-31/")
        file_path = f"data/{stock}/asofdate=2025-03-31/market_data_{attempt}.json"
        write_json(transformed_data, start_time, attempt, file_path)
    
if __name__ == "__main__":
    start_time = time.time()
    logging.basicConfig(filename='tmp/logging.log', level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--tickers", default=os.getenv("POLYGON_TICKERS"))
    parser.add_argument("--APIkey", default=os.getenv("POLYGON_API_KEY"))
    args = parser.parse_args()
    main(args, start_time)