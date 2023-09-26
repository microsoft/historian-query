"""This Python library helps query regularized time series based on raw historian data with
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
"""

from functools import singledispatchmethod
import pyspark.sql.functions as sfn
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from tempo import TSDF

spark = SparkSession.builder.getOrCreate()


class HistorianQuery:
    """This class is used to query regularized time series based on raw historian data on Spark.

    Args:
        df: Spark dataframe with columns `["tag_name", "ts", "value_double", "quality"]`\n
        **OR (alternative initialization)**:\n
        `table_name` _str_ and `tag_list` _list[str]_ - name of the table in the Spark catalog and
        list of tag names to query.
        sample_freq (str): resampled interval length, e.g. "1 minute".
        ff_timeout (str): maximum time interval for forward filling, e.g. "15 minutes", beyond
        which nulls are filled.
        keep_quality (int, list[int] or None): quality codes for which samples are to be kept
        (e.g In GE historian 3 is "good" and in Aveva 192 is "good" quality). There is no default
        as the quality codes depend on the historian. Filtering is strongly encouraged, but setting
        to None will keep all records.
        ignore_nulls (bool): if True, nulls are ignored when resampling, and the last non-null
        value will be used for forward filling (until timeout). Default False.
    Attributes:
        column_types (dict): dictionary of column names and types, used for casting.
        required_cols (list[str]): list of required columns, in the right order.
    """

    @singledispatchmethod  # main scenario - initialize with a Spark DF
    def __init__(self, df: DataFrame, sample_freq, ff_timeout, keep_quality, ignore_nulls=False):
        if not hasattr(self, "table_name"):
            self.table_name = None
            self.tag_list = None
        if not spark.sql(f"select interval {ff_timeout} >= interval {sample_freq}").first()[0]:
            raise ValueError(
                "Timeout interval must be at least as long as sampling frequency, "
                + f"but {ff_timeout} < {sample_freq}."
            )
        self.sample_freq = sample_freq
        self.ff_timeout = ff_timeout
        if not (keep_quality is None or isinstance(keep_quality, (list, int))):
            raise TypeError("only integer, list or None are allowed values for keep_quality")
        self.keep_quality = keep_quality
        self.ignore_nulls = ignore_nulls
        self.column_types = {"value_double": "double", "quality": "integer"}

        # only keep relevant columns
        required_cols = ["tag_name", "ts", "value_double", "quality"]
        self.required_cols = required_cols
        df = df.select(self.required_cols)
        self.df = df

    @__init__.register  # alternative initialization with table name and tag list
    def _(self, table_name: str, tag_list: list[str], *args, **kwargs):
        self.table_name = table_name
        self.tag_list = tag_list
        df = spark.table(table_name).where(sfn.col("tag_name").isin(tag_list))
        self.__init__(df, *args, **kwargs)

    def resample(self, start_ts_str, end_ts_str):
        """Get time series resampled to regular time intervals (this is the main method).

        Args:
            start_ts_str (str): start of the time range
            end_ts_str (str): end of the time range
        Returns:
            Spark dataframe with the resampled data.
        """
        df = self.get_raw_data(start_ts_str, end_ts_str)
        start_ts = self.str2ts(start_ts_str)
        end_ts = self.str2ts(end_ts_str)
        ff_timeout = self.ff_timeout
        sample_freq = self.sample_freq
        ignore_nulls = self.ignore_nulls
        column_types = self.column_types

        if ignore_nulls:
            # we drop nulls so that Tempo takes the last *non-null* within each interval
            # instead of forward filling from a previous interval
            df = df.na.drop(subset=["value_double"])

        # add records with an early timestamp for each tag, so we have records from the start_ts
        first_pad_df = self.pad_constant_timestamp(
            df, start_ts - sfn.expr(f"interval {ff_timeout}")
        )
        df = first_pad_df.union(df)
        # add records with the last timestamp for each tag, so values get ffilled till the end_ts
        last_pad_df = self.pad_constant_timestamp(df, end_ts)
        df = df.union(last_pad_df)
        # add original timestamp converted to number so Tempo can forward fill
        # (and we can recover the original timestamp later)
        df = df.withColumn("orig_ts_double", sfn.col("ts").cast("double"))
        # shift by almost full interval so when Tempo rounds down, we actually get rounded up values
        shift_amount = sfn.expr(f"interval {sample_freq} - interval 1 millisecond")
        df = df.withColumn("ts", sfn.col("ts") + shift_amount)

        if not ignore_nulls:
            # replace null with -Inf, otherwise Tempo will ffill with last non-null
            for col in ["value_double", "quality"]:
                neg_inf = sfn.lit("-Inf").cast(column_types[col])
                df = df.withColumn(
                    col,
                    sfn.when(
                        sfn.col(col).isNull(),
                        neg_inf,
                    ).otherwise(sfn.col(col)),
                )

        df_out = (
            TSDF(
                df,
                partition_cols=["tag_name"],
                ts_col="ts",
            )
            .resample(freq=sample_freq, func="ceil")  # take the last value in each interval
            .interpolate(method="ffill", show_interpolated=False)
            .df
        )

        if not ignore_nulls:
            # convert -Inf back to null
            for col in ["value_double", "quality"]:
                neg_inf = sfn.lit("-Inf").cast(column_types[col])
                df_out = df_out.withColumn(
                    col,
                    sfn.when(
                        sfn.col(col) == neg_inf,
                        sfn.lit(None).cast(column_types[col]),
                    ).otherwise(sfn.col(col)),
                )
        # recover original timestamps
        df_out = df_out.withColumn("orig_ts", sfn.to_timestamp(sfn.col("orig_ts_double")))
        df_out = df_out.drop("orig_ts_double")
        # replace values with null for rows where time since last measurement is at least ff_timeout
        df_out = df_out.withColumn(
            "value_double",
            sfn.when(
                sfn.col("ts") - sfn.col("orig_ts") >= sfn.expr("interval " + ff_timeout), None
            ).otherwise(sfn.col("value_double")),
        )
        # remove records before start time (which were used for forward filling)
        df_out = df_out.filter(sfn.col("ts") >= start_ts)
        # remove the last time interval (logging may not be complete / finalized)
        df_out = df_out.filter(sfn.col("ts") < end_ts)
        # keep only required columns, in the right order
        df_out = df_out.select(self.required_cols + ["orig_ts"])
        return df_out

    def get_raw_data(self, start_ts_str, end_ts_str):
        """Get raw data from historian for the given time range. Note  that the start time gets
        adjusted by subtracting `ff_timeout` to enable forward filling.

        Args:
            start_ts_str (str): start of the time range.
            end_ts_str (str): end of the time range.
        Returns:
            Spark dataframe with the raw data.
        """
        start_ts = self.str2ts(start_ts_str)
        end_ts = self.str2ts(end_ts_str)
        ff_timeout = self.ff_timeout
        keep_quality = self.keep_quality

        df_raw = self.df.where(sfn.col("ts") > start_ts - sfn.expr(f"interval {ff_timeout}"))
        df_raw = df_raw.where(sfn.col("ts") < end_ts)
        # only keep specified quality codes
        if isinstance(keep_quality, list):
            df_raw = df_raw.where(sfn.col("quality").isin(keep_quality))
        elif isinstance(keep_quality, int):
            df_raw = df_raw.where(sfn.col("quality") == keep_quality)
        return df_raw

    def get_latest_ts(self):
        """Get the latest timestamp in the source dataframe. This is intended for use in  scheduled
        runs, as `end_ts_str` in the resample method.

        Returns:
            Timestamp string, e.g. "2022-12-31 12:29:56.460".
        """
        max_ts = self._get_latest_ts(  # for performance, first look in the last 24h
            self.df.filter(self.df.ts >= sfn.date_sub(sfn.current_date(), 1))
        )
        if max_ts is None:  # if no recent records, check the whole df
            max_ts = self._get_latest_ts(self.df)
            if max_ts is None:  # if still nothing, raise an error
                raise EOFError("No records found")
        return max_ts.strftime("%Y-%m-%d %H:%M:%S.%f")

    @staticmethod
    def _get_latest_ts(df):
        """Fetches the max timestamp `ts` from a dataframe.

        Args:
            df (Spark dataframe): dataframe from which the max timestamp is extracted.
        Returns:
            Timestamp or None (if no records found).
        """
        max_ts = df.select(sfn.max(sfn.col("ts")).alias("max_ts")).first()[0]
        return max_ts

    def pad_constant_timestamp(self, df, ts):
        """Creates records with a constant timestamp for each tag, with nulls for value and quality.

        Args:
            df (Spark dataframe): dataframe from which the distinct tag names are extracted
            ts (str): timestamp string
        Returns:
            Spark dataframe.
        """
        pad_df = df.select("tag_name").distinct().withColumn("ts", ts)
        for col in ["value_double", "quality"]:
            pad_df = pad_df.withColumn(col, sfn.lit(None).cast(self.column_types[col]))
        return pad_df

    @staticmethod
    def str2ts(ts_str):
        """Convert a timestamp string to a Spark timestamp.

        Args:
            ts_str (str): timestamp string, e.g. "2022-12-31 12:30:00.000"
        Returns:
            Spark timestamp.
        """
        return sfn.to_timestamp(sfn.lit(ts_str))
