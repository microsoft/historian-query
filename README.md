# historian\_query

This Python library helps query regularized time series based on raw historian data with
irregular time intervals (due to compression, deadband, outages etc.) on Spark.

The **HistorianQuery** class is a wrapper around the **TSDF** class in the Databricks
[Tempo](https://databrickslabs.github.io/tempo/user-guide.html) library. It adds some functionality
that is helpful for querying data from a historian in manufacturing context:

* Returns records from start to end time (instead of just between first and last record for each
tag).
* Interval timestamp is rounded up instead of down, reporting the last known value at the given
time point.
* The original timestamp of the observation is also reported.
* Timeout functionality - forward fill only up to a specified time interval to avoid stale values.
* Filter by quality flag - historians often capture an indication of how reliable each observation
is. This is to remind users to keep only good quality observations.
* Keep or ignore nulls - a null value recorded by a historian can be an indication that the last
known value should no longer be forward filled.

**Example usage:**

```python
from historian_query import HistorianQuery

HQ = HistorianQuery(df=df, sample_freq="1 minute", ff_timeout="15 minutes",
    keep_quality=[3], ignore_nulls=False)
resampled_df = HQ.resample(start_ts_str="2023-01-01 09:00:00",
    end_ts_str="2023-01-01 18:00:00")
```

## HistorianQuery

```python
class HistorianQuery()
```

This class is used to query regularized time series based on raw historian data on Spark.

**Arguments**:

- `df` - Spark dataframe with columns `["tag_name", "ts", "value_double", "quality"]`
  
  **OR (alternative initialization)**:
  
  `table_name` _str_ and `tag_list` _list[str]_ - name of the table in the Spark catalog and
  list of tag names to query.
- `sample_freq` _str_ - resampled interval length, e.g. "1 minute".
- `ff_timeout` _str_ - maximum time interval for forward filling, e.g. "15 minutes", beyond
  which nulls are filled.
- `keep_quality` _int, list[int] or None_ - quality codes for which samples are to be kept
  (e.g In GE historian 3 is "good" and in Aveva 192 is "good" quality). There is no default
  as the quality codes depend on the historian. Filtering is strongly encouraged, but setting
  to None will keep all records.
- `ignore_nulls` _bool_ - if True, nulls are ignored when resampling, and the last non-null
  value will be used for forward filling (until timeout). Default False.

**Attributes**:

- `column_types` _dict_ - dictionary of column names and types, used for casting.
- `required_cols` _list[str]_ - list of required columns, in the right order.

### resample

```python
def resample(start_ts_str, end_ts_str)
```

Get time series resampled to regular time intervals (this is the main method).

**Arguments**:

- `start_ts_str` _str_ - start of the time range
- `end_ts_str` _str_ - end of the time range

**Returns**:

  Spark dataframe with the resampled data.

### get\_raw\_data

```python
def get_raw_data(start_ts_str, end_ts_str)
```

Get raw data from historian for the given time range. Note  that the start time gets
adjusted by subtracting `ff_timeout` to enable forward filling.

**Arguments**:

- `start_ts_str` _str_ - start of the time range.
- `end_ts_str` _str_ - end of the time range.

**Returns**:

  Spark dataframe with the raw data.

### get\_latest\_ts

```python
def get_latest_ts()
```

Get the latest timestamp in the source dataframe. This is intended for use in  scheduled
runs, as `end_ts_str` in the resample method.

**Returns**:

  Timestamp string, e.g. "2022-12-31 12:29:56.460".

### pad\_constant\_timestamp

```python
def pad_constant_timestamp(df, ts)
```

Creates records with a constant timestamp for each tag, with nulls for value and quality.

**Arguments**:

- `df` _Spark dataframe_ - dataframe from which the distinct tag names are extracted
- `ts` _str_ - timestamp string

**Returns**:

  Spark dataframe.

### str2ts

```python
@staticmethod
def str2ts(ts_str)
```

Convert a timestamp string to a Spark timestamp.

**Arguments**:

- `ts_str` _str_ - timestamp string, e.g. "2022-12-31 12:30:00.000"

**Returns**:

  Spark timestamp.
