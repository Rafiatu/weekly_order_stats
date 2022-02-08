import pandas as pd
import pyspark
from pyspark.sql import SparkSession, functions as F
from utils import mode, count_if, sum_size_if, sum_profit_if, sum_revenue_if


class CustomerOrders:
    def __init__(self, path_to_excel_file: str) -> None:
        self.aggregate_dataframe = None
        self.dataframe = None
        self.__spark = SparkSession.builder.getOrCreate()
        self.spark_df = None
        self.input_path = path_to_excel_file

    def extract_data(self) -> pyspark.sql.dataframe.DataFrame:
        """
        Reads and returns a dataframe from an excel spreadsheet.
        :return spark_df: Spark dataframe with added column `week`
        """
        try:
            df = pd.read_excel(self.input_path, sheet_name="orders_daily_sample")
            spark_df = self.__spark.createDataFrame(df)
            self.spark_df = spark_df.withColumn("week", F.weekofyear(spark_df.dt))
            return self.spark_df
        except Exception as error:
            print("There was a problem with reading the data.")
            raise error

    def transform_data(self) -> pyspark.sql.dataframe.DataFrame:
        """
        Transforms the data by performing grouping and aggregation functions to
        return the weekly order statistics.
        :return aggregate_dataframe: The transformed data after all aggregations.
        """
        try:
            self.aggregate_dataframe = (
                self.spark_df.groupBy("week", "customer_id")
                .agg(
                    count_if(F.col("order_type") == "Delivered").alias(
                        "order_executed_count"
                    ),
                    count_if(F.col("order_type") == "Placed").alias(
                        "order_placed_count"
                    ),
                    count_if(F.col("order_type") == "Returned").alias(
                        "order_returned_count"
                    ),
                    F.avg("order_size").alias("order_size_avg"),
                    sum_size_if(F.col("order_type") == "Delivered").alias(
                        "order_size_executed_sum"
                    ),
                    sum_size_if(F.col("order_type") == "Placed").alias(
                        "order_size_placed_sum"
                    ),
                    sum_size_if(F.col("order_type") == "Returned").alias(
                        "order_size_returned_sum"
                    ),
                    F.sum("order_revenue").alias("order_revenue_sum"),
                    F.avg("order_revenue").alias("order_revenue_avg"),
                    sum_revenue_if(F.col("order_type") == "Delivered").alias(
                        "order_revenue_executed_sum"
                    ),
                    sum_revenue_if(F.col("order_type") == "Placed").alias(
                        "order_revenue_placed_sum"
                    ),
                    sum_revenue_if(F.col("order_type") == "Returned").alias(
                        "order_revenue_returned_sum"
                    ),
                    F.sum("order_profit").alias("order_profit_sum"),
                    F.avg("order_profit").alias("order_profit_avg"),
                    sum_profit_if(F.col("order_type") == "Delivered").alias(
                        "order_profit_executed_sum"
                    ),
                    sum_profit_if(F.col("order_type") == "Placed").alias(
                        "order_profit_placed_sum"
                    ),
                    sum_profit_if(F.col("order_type") == "Returned").alias(
                        "order_profit_returned_sum"
                    ),
                    F.max("discount_active").alias("discount_active"),
                    F.avg("discount_amount").alias("discount_amount_avg"),
                    F.last("address_id").alias("last_address"),
                    mode(F.collect_list("address_id")).alias("top_address"),
                    mode(F.collect_list("payment_source")).alias("payment_source_top"),
                    F.avg("order_delivery_fee").alias("delivery_fee_avg"),
                    count_if(F.col("order_delivery_fee") > 0).alias(
                        "order_w_delivery_fee"
                    ),
                    F.avg("order_packaging_fee").alias("packaging_fee_avg"),
                    count_if(F.col("order_packaging_fee") > 0).alias(
                        "order_w_packaging_fee"
                    ),
                    F.date_format(F.first("dt"), "yyyyMMdd").alias("dt"),
                )
                .orderBy("week")
            ).drop("week")
            return self.aggregate_dataframe
        except Exception as error:
            print(
                "An error occurred while transforming the data.",
                "Returning the untransformed data",
            )
            raise error

    def load_data_to_csv(self, output_path: str) -> bool:
        """
        Saves the transformed dataframe to the csv file path provided.
        :param output_path: The path to where the transformed data should be stored.
        :return: None. Simply creates / updates the csv file in the specified path.
        """
        try:
            if not output_path.endswith(".csv"):
                raise ValueError
            pandas_df: pd.DataFrame = self.aggregate_dataframe.toPandas()
            pandas_df.to_csv(output_path, index=False)
            print(f"File successfully saved in {output_path}")
            return True
        except Exception as error:
            print("Unable to save transformed data.")
            raise error
