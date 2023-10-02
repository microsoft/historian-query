# historian\_query

## HistorianQuery

```python
class HistorianQuery()
```

This class is used to query regularized time series based on raw historian data on Spark.

**Methods**:

- `resample` - get time series resampled to regular time intervals (this is the main method).
- `get_raw_data` - get underlying raw data from historian.
- `get_latest_ts` - get the latest timestamp in the source dataframe.
- `pad_constant_timestamp` - create records with a constant timestamp for each tag.
- `str2ts` - convert a timestamp string to a Spark timestamp.

**Attributes**:

- `column_types` _dict_ - dictionary of column names and types, used for casting.
- `required_cols` _list[str]_ - list of required columns, in the right order.

### \_\_init\_\_

```python
@singledispatchmethod
def __init__(df: DataFrame,
             sample_freq,
             ff_timeout,
             keep_quality,
             ignore_nulls=False)
```

**Arguments**:

- `df` - Spark dataframe with columns `["tag_name", "ts", "value_double", "quality"]`
  
  **OR (alternative initialization; NOTE: the first argument must be positional)**:
  
  `table_name` _str_ and `tag_list` _list[str]_ - name of the table in the Spark catalog
  and list of tag names to query.
- `sample_freq` _str_ - resampled interval length, e.g. "1 minute".
- `ff_timeout` _str_ - maximum time interval for forward filling, e.g. "15 minutes", beyond
  which nulls are filled.
- `keep_quality` _int, list[int] or None_ - quality codes for which samples are to be kept
  (e.g In GE historian 3 is "good" and in Aveva 192 is "good" quality). There is no
  default as the quality codes depend on the historian. Filtering is strongly encouraged,
  but setting to None will keep all records.
- `ignore_nulls` _bool_ - if True, nulls are ignored when resampling, and the last non-null
  value will be used for forward filling (until timeout). Default False.

### resample

```python
def resample(start_ts_str, end_ts_str)
```

Get time series resampled to regular time intervals.

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
