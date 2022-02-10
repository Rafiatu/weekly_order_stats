from collections import Counter
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from typing import Union


count_if = lambda cond: F.sum(F.when(cond, 1).otherwise(0))

sum_size_if = lambda cond: F.sum(F.when(cond, F.col("order_size")).otherwise(0))

sum_revenue_if = lambda cond: F.sum(F.when(cond, F.col("order_revenue")).otherwise(0))

sum_profit_if = lambda cond: F.sum(F.when(cond, F.col("order_profit")).otherwise(0))


def find_mode(column: Union[list, str]) -> Union[str, int]:
    counts = Counter(column)
    top = counts.most_common(1)
    return top[0][0]


mode: F.udf = F.udf(find_mode, StringType())
