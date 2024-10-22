from typing import TYPE_CHECKING

from pyspark.sql.observation import Observation

if TYPE_CHECKING:
    from typing import Callable, Sequence, List, Tuple

    from pyspark.sql import Column, DataFrame

    TransformationFunction = Callable[..., DataFrame]


OBSERVATIONS: "List[Tuple[TransformationFunction, Observation]]" = []


def observe_output_df(aggregation_columns_factory: "Callable[[DataFrame], Sequence[Column]]"):
    def decorator(transformation_function: "TransformationFunction"):
        def wrapper(*args, **kwargs):
            df = transformation_function(*args, **kwargs)
            aggregation_columns = aggregation_columns_factory(df)
            observation = Observation()
            OBSERVATIONS.append((transformation_function, observation))
            observed_df = df.observe(observation, *aggregation_columns)
            return observed_df

        return wrapper

    return decorator
