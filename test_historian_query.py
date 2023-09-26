import pytest
from tempo.utils import ResampleWarning
import pandas as pd
import numpy as np
import pyspark.sql.functions as sfn

from historian_query import HistorianQuery, spark


# @pytest.fixture
def historian_df():
    df = pd.DataFrame(
        columns=["tag_name", "ts", "quality", "value_double"],
        data=[
            ("Power_kW", "2023-01-01 11:58:13.151", 3, 9.1377),
            ("Power_kW", "2023-01-01 11:58:40.085", 3, 10.5673),
            ("Power_kW", "2023-01-01 12:00:37.603", 3, 11.2904),
            ("Power_kW", "2023-01-01 12:01:04.539", 3, 12.3146),
            ("Power_kW", "2023-01-01 12:01:11.043", 1, 15.0384),
            ("Power_kW", "2023-01-01 12:01:22.006", 3, None),
            ("Power_kW", "2023-01-01 12:01:47.072", 3, None),
            ("Power_kW", "2023-01-01 12:02:18.180", 3, 8.3791),
            ("Power_kW", "2023-01-01 12:03:11.831", 3, 7.2956),
            ("Power_kW", "2023-01-01 12:03:51.832", 3, 7.0819),
            ("Power_kW", "2023-01-01 12:04:52.566", 3, 6.9627),
            ("Power_kW", "2023-01-01 12:05:42.517", 3, None),
            ("STEAM_PRESSURE.F_CV", "2023-01-01 12:00:56.842", 3, 125.5432),
            ("STEAM_PRESSURE.F_CV", "2023-01-01 12:03:06.842", 3, 134.1618),
            ("STEAM_PRESSURE.F_CV", "2023-01-01 12:03:12.238", 1, 130.5387),
            ("belt_speed", "2023-01-01 11:58:36.391", 3, None),
            ("belt_speed", "2023-01-01 12:00:21.086", 3, 0.765),
            ("belt_speed", "2023-01-01 12:01:03.215", 3, 0.648),
            ("belt_speed", "2023-01-01 12:01:31.763", 3, 0.8965),
            ("belt_speed", "2023-01-01 12:01:36.206", 3, None),
            ("belt_speed", "2023-01-01 12:02:22.269", 3, 0.3415),
            ("belt_speed", "2023-01-01 12:05:15.049", 3, 0.3638),
            ("temperature", "2023-01-01 11:57:00.000", 3, 165),
        ],
    )

    df["ts"] = pd.to_datetime(df["ts"], format="%Y-%m-%d %H:%M:%S.%f")
    df = spark.createDataFrame(df)
    # pandas infers numeric dtype and converts None to NaN
    # we want 'null' entries in the Spark df
    df = df.replace(float("nan"), None)
    return df


historian_df = historian_df()  # imitate "fixture" interface without passing it as argument

# Default test parameters
sample_freq = "1 minute"
ff_timeout = "5 minutes"
start_ts_str = "2023-01-01 12:00:00"
end_ts_str = "2023-01-01 12:05:00"
keep_quality = 3
ignore_nulls = False


class HistorianQueryDefault(HistorianQuery):
    def __init__(
        self,
        df=historian_df,
        sample_freq=sample_freq,
        ff_timeout=ff_timeout,
        keep_quality=keep_quality,
        ignore_nulls=ignore_nulls,
    ):
        super().__init__(
            df,
            sample_freq=sample_freq,
            ff_timeout=ff_timeout,
            keep_quality=keep_quality,
            ignore_nulls=ignore_nulls,
        )


# test the quality flag filter
@pytest.mark.parametrize("keep_quality, expected", [(3, 19), ([1], 2), (None, 21)])
def test_raw_rec_count(keep_quality, expected):
    HQ = HistorianQueryDefault(keep_quality=keep_quality)
    raw_df = HQ.get_raw_data(start_ts_str, end_ts_str)
    assert raw_df.count() == expected


# test the sample frequency parameter
@pytest.mark.parametrize("sample_freq", ["5 seconds", "1 minute", "5 minutes"])
def test_resampled_rec_count(sample_freq):
    HQ = HistorianQueryDefault(sample_freq=sample_freq)
    with pytest.warns(ResampleWarning):  # to hide the warning
        resampled_df = HQ.resample(start_ts_str, end_ts_str)
    tag_cnt = historian_df.select("tag_name").distinct().count()
    expected = tag_cnt * np.floor(
        (pd.to_datetime(end_ts_str) - pd.to_datetime(start_ts_str)) / pd.Timedelta(sample_freq)
    )
    assert resampled_df.count() == expected


# check that records before start and after end get removed
@pytest.mark.parametrize(
    "ff_timeout, start_ts_str, end_ts_str",
    [
        (ff_timeout, start_ts_str, end_ts_str),
        ("1 minute", start_ts_str, end_ts_str),
        ("1 minute", "2023-01-01 12:03:00", end_ts_str),
        ("2 minutes", start_ts_str, "2023-01-01 12:10:00"),
    ],
)
def test_start_end(ff_timeout, start_ts_str, end_ts_str):
    HQ = HistorianQueryDefault(ff_timeout=ff_timeout)
    raw_df = HQ.get_raw_data(start_ts_str, end_ts_str)
    assert raw_df.selectExpr("min(ts)").first()[0] > pd.to_datetime(start_ts_str) - pd.to_timedelta(
        ff_timeout
    )
    assert raw_df.selectExpr("max(ts)").first()[0] < pd.to_datetime(end_ts_str)
    with pytest.warns(ResampleWarning):  # to hide the warning
        resampled_df = HQ.resample(start_ts_str, end_ts_str)
    assert resampled_df.selectExpr("min(ts)").first()[0] == pd.to_datetime(start_ts_str)
    assert resampled_df.selectExpr("max(ts)").first()[0] < pd.to_datetime(end_ts_str)


# check if the ignore_nulls flag works
@pytest.mark.parametrize("ignore_nulls, expected", [(True, 12.3146), (False, None)])
def test_ignore_nulls(ignore_nulls, expected):
    HQ = HistorianQueryDefault(ignore_nulls=ignore_nulls)
    with pytest.warns(ResampleWarning):  # to hide the warning
        resampled_df = HQ.resample(start_ts_str, end_ts_str)
    val = (
        resampled_df.where(sfn.col("tag_name") == "Power_kW")
        .where(sfn.col("ts") == "2023-01-01 12:02:00.000")
        .first()["value_double"]
    )
    if expected is None:
        assert val is None
    else:
        assert val == expected


# test timeout works
@pytest.mark.parametrize(
    "ff_timeout, expected",
    [("1 minute", 0), ("3 minutes", 0), ("5 minutes", 2), ("10 minutes", 5)],
)
def test_timeout(ff_timeout, expected):
    HQ = HistorianQueryDefault(ff_timeout=ff_timeout)
    with pytest.warns(ResampleWarning):  # to hide the warning
        resampled_df = HQ.resample(start_ts_str, end_ts_str)
    val_cnt = (
        resampled_df.where(sfn.col("tag_name") == "temperature")
        .where(sfn.col("value_double").isNotNull())
        .count()
    )
    assert val_cnt == expected


# check method to get latest timestamp
def test_get_latest():
    HQ = HistorianQueryDefault()
    latest_ts = historian_df.selectExpr("max(ts)").first()[0]
    latest_ts = str(latest_ts)
    assert HQ.get_latest_ts() == latest_ts
