# Historian Query

This Python library helps query regularized time series based on raw historian data with
irregular time intervals (due to compression, deadband, outages etc.) on Spark. It is particularly
suited for compressed manufacturing time series coming from a historian database, and it imitates the
resampling functionality as offered by some historians.

## Useful Links

- [Historian Query reference](reference.md)
- [Databricks Tempo documentation](https://databrickslabs.github.io/tempo/)

## Example usage

An input table / dataframe with columns `["tag_name", "ts", "value_double", "quality"]` is required.
This is then loaded and resampled as follows:

```python
from historian_query import HistorianQuery

table_name = "ot_database.historian_timeseries"
tag_dict = {
    "PR03.229HIC0384_SPEED_PV": "belt_speed",
    "PR03.730KIC4985_FLOW_PV": "water_flow",
    "PR03.849ZMC3525_PRESS_PV": "pressure",
}
HQ = HistorianQuery(
    table_name=table_name,
    tag_list=list(tag_dict.keys()),
    sample_freq="10 seconds",
    ff_timeout="15 minutes",
    keep_quality=3,
)
df = HQ.resample("2023-08-01 00:00:00", "2023-08-02 00:00:00")
```

Note that instead of `table_name` and `tag_list` you can alternatively pass a Spark dataframe `df`,
which allows for more flexibility for your data source (see the [reference](reference.md)). The
output is a resampled Spark dataframe in long format:

| tag_name                 | ts                      | value_double | quality | orig_ts                 |
|--------------------------|-------------------------|-------------:|--------:|-------------------------|
| PR03.229HIC0384_SPEED_PV | 2023-08-01T00:00:00.000 | 3564.920410  | 3       | 2023-07-31T23:59:53.000 |
| PR03.730KIC4985_FLOW_PV  | 2023-08-01T00:00:00.000 | 53.218196    | 3       | 2023-07-31T23:59:58.000 |
| PR03.849ZMC3525_PRESS_PV | 2023-08-01T00:00:00.000 | 7.485432     | 3       | 2023-07-31T23:59:58.000 |
| PR03.229HIC0384_SPEED_PV | 2023-08-01T00:00:10.000 | 3565.104004  | 3       | 2023-08-01T00:00:03.000 |
| PR03.730KIC4985_FLOW_PV  | 2023-08-01T00:00:10.000 | 53.218196    | 3       | 2023-07-31T23:59:58.000 |
| PR03.849ZMC3525_PRESS_PV | 2023-08-01T00:00:10.000 | 8.028755     | 3       | 2023-08-01T00:00:08.000 |
| PR03.229HIC0384_SPEED_PV | 2023-08-01T00:00:20.000 | 3565.104004  | 3       | 2023-08-01T00:00:03.000 |
| PR03.730KIC4985_FLOW_PV  | 2023-08-01T00:00:20.000 | 53.218196    | 3       | 2023-07-31T23:59:58.000 |
| PR03.849ZMC3525_PRESS_PV | 2023-08-01T00:00:20.000 | 7.371169     | 3       | 2023-08-01T00:00:18.000 |
| ...                      |                         |              |         |                         |

Each tag now has observations every 10 seconds. Note that the original time stamps are are slightly
earlier, and some have been filled forward several times (no newer observation available).

We can now use standard Spark tools for aggregation and feature engineering, e.g. average over 5
minute windows:

```python
from itertools import chain
from pyspark.sql.functions import window, avg, create_map, lit, col

# aggregate over 5 minute windows
df = df.groupBy("tag_name", window("ts", "5 minutes").end.alias("ts")).agg(
    avg("value_double").alias("avg_val")
)
# map tag names
tag_map = create_map([lit(x) for x in chain(*tag_dict.items())])
df = df.withColumn("tag_descr", tag_map[col("tag_name")])
# pivot to get tags as columns
df.toPandas().pivot(index="ts", columns="tag_descr", values="avg_val")
```

The output is now in a suitable format for using in reports or ML models:

|                  ts |  belt_speed | pressure | water_flow |
|--------------------:|------------:|---------:|-----------:|
| 2023-08-01 00:05:00 | 3564.950122 | 7.774326 |  53.010570 |
| 2023-08-01 00:10:00 | 3565.053158 | 7.812656 |  53.042973 |
| 2023-08-01 00:15:00 | 3564.626245 | 7.869692 |  52.981631 |
| 2023-08-01 00:20:00 | 3564.934269 | 7.828228 |  52.985406 |
|...                  |             |          |            |

Note that we use the end-point timestamp for each aggregation interval, so that each record is only
based on data that is available at the specified timestamp.

To summarize we recommend a two-step approach:

1) resample to a fine time granularity,
2) aggregate to desired time windows.

This ensures that the potentially irregular observations from raw historian data do not bias
the aggregation (more weight on volatile periods) or lead to skipped/null intervals (due to gaps).

## How this works

The **HistorianQuery** class is a wrapper around the **TSDF** class in the Databricks
[Tempo](https://databrickslabs.github.io/tempo/user-guide.html) library. It adds some functionality
that is helpful for querying data from a historian in manufacturing context:

- Returns records from start to end time (instead of just between first and last record for each
tag).
- Interval timestamp is rounded up instead of down, reporting the last known value at the given
time point.
- The original timestamp of the observation is also reported.
- Timeout functionality - forward fill only up to a specified time interval to avoid stale values.
- Filter by quality flag - historians often capture an indication of how reliable each observation
is. This is to remind users to keep only good quality observations.
- Keep or ignore nulls - a null value recorded by a historian can be an indication that the last
known value should no longer be forward filled.

While Tempo offers other options, here the resampling configuration has been fixed to the following :

- Interpolation: _ffill_ - forwards fill
- Aggregation: _ceil_ - returns the latest value by timestamp.

Forward filling (along with rounding up interval timestamps) ensures that observations are only
based on past data. This is important for e.g. Machine Learning use-cases, where the training
samples must contain only information that is available at inference time during production.

Taking the last known value instead of _mean_ ensures that the value makes sense for any tag, e.g.
also for 0/1 indicators. For custom aggregation, follow the approach described in
[Example Usage](#example-usage).
