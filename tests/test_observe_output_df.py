import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from observe_data_validation import observe_output_df, OBSERVATIONS


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("test").master("local[*]").getOrCreate()


@pytest.fixture(scope='module')
def sample_df(spark):
    return spark.range(100)


def test_observe_output_df_count(spark, sample_df):
    filter_value = 50

    def aggregation_columns_factory(df):
        return [F.count('*').alias('count')]

    @observe_output_df(aggregation_columns_factory)
    def sample_transformation_function(df):
        return df.filter(F.col("id") < filter_value)

    result_df = sample_transformation_function(sample_df)
    result_df.count()

    assert len(OBSERVATIONS) == 1
    assert len(OBSERVATIONS[0]) == 2
    assert OBSERVATIONS[0][0].__name__ == 'sample_transformation_function'
    assert OBSERVATIONS[0][1].get["count"] == filter_value


@pytest.mark.parametrize('bad_aggregation_columns_factory, error_match_string', [
    (
            lambda df: [F.col('id')],
            'INVALID_OBSERVED_METRICS.NON_AGGREGATE_FUNC_ARG_IS_ATTRIBUTE'
    ),
    (
            lambda df: [F.lag('id', 1).over(Window().orderBy('id'))],
            'INVALID_OBSERVED_METRICS.WINDOW_EXPRESSIONS_UNSUPPORTED'
    ),
], ids=['returns non-agg column', 'returns window function'])
def test_aggregation_columns_factory_returns_non_agg_columns(spark, sample_df, bad_aggregation_columns_factory,
                                                             error_match_string):
    @observe_output_df(bad_aggregation_columns_factory)
    def sample_transformation_function(df):
        return df.filter(F.col("id") < 50)

    with pytest.raises(AnalysisException, match=error_match_string):
        sample_transformation_function(sample_df)
