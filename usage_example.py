from typing import TYPE_CHECKING

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from observe_data_validation import OBSERVATIONS, observe_output_df

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, Column
    from typing import List

spark = SparkSession.builder.appName("example").master("local[*]").getOrCreate()


# Define aggregation functions generator
def count_nulls_for_each_column(df: "DataFrame") -> "List[Column]":
    return [F.count("*").alias("count")] + [F.count_if(F.col(column).isNull()).alias(f'nulls_in_{column}') for column in
                                            df.columns]


# Define transformation functions
@observe_output_df(count_nulls_for_each_column)
def identity_df(df: "DataFrame") -> "DataFrame":
    return df


@observe_output_df(count_nulls_for_each_column)
def filter_half_df(df: "DataFrame") -> "DataFrame":
    return df.filter(F.col("id") < 50)


@observe_output_df(count_nulls_for_each_column)
def add_random_null_column(df: "DataFrame", column_name, chance_to_be_null=0.5) -> "DataFrame":
    return df.withColumn(column_name,
                         F.when((F.rand() * 100).cast('int') % int(1 / chance_to_be_null) == 0, 'not_null').otherwise(
                             F.lit(None)))


@observe_output_df(count_nulls_for_each_column)
def filter_quarter_df(df: "DataFrame") -> "DataFrame":
    return df.filter(F.col("id") < 25)


# Initialize dataframe and run transformations
my_df = spark.range(100)
my_df = identity_df(my_df)
my_df = filter_half_df(my_df)
my_df = add_random_null_column(my_df, 'nulls1')
my_df = filter_quarter_df(my_df)
my_df = add_random_null_column(my_df, 'nulls2')

# Run an action that will cause a shuffle and report metrics to the observation
my_df.count()

# Print the observed data for each function in the order they were called
for function, observation in OBSERVATIONS:
    print(f"{function.__name__}: {observation.get}")
