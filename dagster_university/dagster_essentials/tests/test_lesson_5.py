import dagster as dg

from src.dagster_essentials.completed.lesson_5.defs.assets import metrics, trips


def test_assets():
    assets = [
        trips.taxi_trips_file,
        trips.taxi_zones_file,
        trips.taxi_trips,
        trips.taxi_zones,
        metrics.trips_by_week,
        metrics.manhattan_stats,
        metrics.manhattan_map,
    ]
    result = dg.materialize(assets)
    assert result.success
