import dagster as dg

import matplotlib.pyplot as plt
import geopandas as gpd
import pandas as pd


import os

from dagster_essentials.defs.assets import constants
from dagster_essentials.defs.partitions import weekly_partition

from datetime import datetime, timedelta
from dagster_duckdb import DuckDBResource


@dg.asset(
    deps=["taxi_trips", "taxi_zones"]
)
def manhattan_stats(database: DuckDBResource) -> None:
    query = """
        select
            zones.zone,
            zones.borough,
            zones.geometry,
            count(1) as num_trips,
        from trips
        left join zones on trips.pickup_zone_id = zones.zone_id
        where borough = 'Manhattan' and geometry is not null
        group by zone, borough, geometry
    """

    with database.get_connection() as conn:
        trips_by_zone = conn.execute(query).fetch_df()

    trips_by_zone["geometry"] = gpd.GeoSeries.from_wkt(trips_by_zone["geometry"])
    trips_by_zone = gpd.GeoDataFrame(trips_by_zone)

    with open(constants.MANHATTAN_STATS_FILE_PATH, 'w') as output_file:
        output_file.write(trips_by_zone.to_json())

@dg.asset(
    deps=["manhattan_stats"]
)
def manhattan_map() -> None:
    trips_by_zone = gpd.read_file(constants.MANHATTAN_STATS_FILE_PATH)

    fig, ax = plt.subplots(figsize=(10, 10))
    trips_by_zone.plot(column="num_trips", cmap="plasma", legend=True, ax=ax, edgecolor="black")
    ax.set_title("Number of Trips per Taxi Zone in Manhattan")

    ax.set_xlim([-74.05, -73.90])  # Adjust longitude range
    ax.set_ylim([40.70, 40.82])  # Adjust latitude range
    
    # Save the image
    plt.savefig(constants.MANHATTAN_MAP_FILE_PATH, format="png", bbox_inches="tight")
    plt.close(fig)


@dg.asset(
    deps=["taxi_trips"],
    partitions_def=weekly_partition
)
def trips_by_week(context: dg.AssetExecutionContext, database: DuckDBResource) -> None:
    
    period_to_process = context.partition_key
    query = f"""
            SELECT 
                date_trunc('week', pickup_datetime) AS period,
                COUNT(1) AS num_trips,
                sum(passenger_count) AS passenger_count,
                sum(total_amount) AS total_amount,
                sum(trip_distance) AS trip_distance
            from trips
            WHERE pickup_datetime >= '{period_to_process}'::date
            and pickup_datetime < '{period_to_process}'::date + interval '1 week'
            GROUP BY date_trunc('week', pickup_datetime)
    """
    with database.get_connection() as conn:
        trips_by_week = conn.execute(query).fetch_df()
    fileName = constants.TRIPS_BY_WEEK_FILE_PATH[:-4] + period_to_process + ".csv"
    trips_by_week.to_csv(fileName, index= 0)

@dg.asset(
    deps=["taxi_trips"],
    partitions_def=weekly_partition
)
def trips_by_week_tut(context: dg.AssetExecutionContext, database: DuckDBResource) -> None:
    """
      The number of trips per week, aggregated by week.
    """

    period_to_fetch = context.partition_key

    # get all trips for the week
    query = f"""
        select vendor_id, total_amount, trip_distance, passenger_count
        from trips
        where pickup_datetime >= '{period_to_fetch}'
            and pickup_datetime < '{period_to_fetch}'::date + interval '1 week'
    """

    with database.get_connection() as conn:
        data_for_month = conn.execute(query).fetch_df()

    aggregate = data_for_month.agg({
        "vendor_id": "count",
        "total_amount": "sum",
        "trip_distance": "sum",
        "passenger_count": "sum"
    }).rename({"vendor_id": "num_trips"}).to_frame().T # type: ignore

    # clean up the formatting of the dataframe
    aggregate["period"] = period_to_fetch
    aggregate['num_trips'] = aggregate['num_trips'].astype(int)
    aggregate['passenger_count'] = aggregate['passenger_count'].astype(int)
    aggregate['total_amount'] = aggregate['total_amount'].round(2).astype(float)
    aggregate['trip_distance'] = aggregate['trip_distance'].round(2).astype(float)
    aggregate = aggregate[["period", "num_trips", "total_amount", "trip_distance", "passenger_count"]]

    try:
        # If the file already exists, append to it, but replace the existing month's data
        existing = pd.read_csv("data/outputs/trips_by_week_tut.csv")
        existing = existing[existing["period"] != period_to_fetch]
        existing = pd.concat([existing, aggregate]).sort_values(by="period")
        existing.to_csv("data/outputs/trips_by_week_tut.csv", index=False)
    except FileNotFoundError:
        aggregate.to_csv("data/outputs/trips_by_week_tut.csv", index=False)