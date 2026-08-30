from pathlib import Path

import requests
from dagster_essentials.defs.assets import constants
import dagster as dg
from dagster_duckdb import DuckDBResource
from dagster_essentials.defs.partitions import monthly_partition


@dg.asset(partitions_def=monthly_partition)
def taxi_trips_file(context: dg.AssetExecutionContext) -> None:
    """
    The raw parquet files for the taxi trips dataset. Sourced from the NYC Open Data portal.
    """
    partition_date_str = context.partition_key  # YYYY-MM-DD
    month_to_fetch = partition_date_str[:-3]  # YYYY-MM

    persisted_file_path = Path(
        constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)
    )
    if persisted_file_path.exists():
        return None

    raw_trips = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    )
    persisted_file_path.parent.mkdir(parents=True, exist_ok=True)
    persisted_file_path.write_bytes(raw_trips.content)


@dg.asset
def taxi_zones_file() -> None:
    """
    The csv file for taxi zones of NYC
    """
    TAXI_ZONES_URL = "https://community-engineering-artifacts.s3.us-west-2.amazonaws.com/dagster-university/data/taxi_zones.csv"

    persisted_file_path = Path(constants.TAXI_ZONES_FILE_PATH)
    if persisted_file_path.exists():
        return None

    persisted_file_path.parent.mkdir(parents=True, exist_ok=True)
    persisted_file_path.write_bytes(requests.get(TAXI_ZONES_URL).content)
    return None


# src/dagster_essentials/defs/assets/trips.py
@dg.asset(deps=["taxi_trips_file"], partitions_def=monthly_partition)
def taxi_trips(
    context: dg.AssetExecutionContext,
    database: DuckDBResource,
) -> None:
    """
    The raw taxi trips dataset, loaded into a DuckDB database
    """
    partition_date_str = context.partition_key  # YYYY-MM-DD
    month_to_fetch = partition_date_str[:-3]  # YYYY-MM

    query = f"""
        create table if not exists trips (
      vendor_id integer, pickup_zone_id integer, dropoff_zone_id integer,
      rate_code_id double, payment_type integer, dropoff_datetime timestamp,
      pickup_datetime timestamp, trip_distance double, passenger_count double,
      total_amount double, partition_date varchar
    );
    
    delete from trips where partition_date = '{month_to_fetch}';

    insert into trips
    select
      VendorID, PULocationID, DOLocationID, RatecodeID, payment_type, tpep_dropoff_datetime,
      tpep_pickup_datetime, trip_distance, passenger_count, total_amount, '{month_to_fetch}' as partition_date
    from '{constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)}';
    """
    with database.get_connection() as conn:
        conn.execute(query)


@dg.asset(deps=["taxi_zones_file"])
def taxi_zones(database: DuckDBResource) -> None:
    """
    The raw taxi zone dataset, loaded into a DuckDB database
    """
    query = f"""
        create or replace table zones as (
          select
            LocationID as zone_id,
            zone,
            borough,
            the_geom as geometry,
          from '{constants.TAXI_ZONES_FILE_PATH}'
        );
    """
    with database.get_connection() as conn:
        conn.execute(query)
