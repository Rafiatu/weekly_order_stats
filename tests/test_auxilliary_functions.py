import logging
from pyspark.sql import functions as F
from src.processor import CustomerOrders
from unittest import TestCase
from utils import *
import warnings


logging.disable(logging.CRITICAL)


class UtilsTest(TestCase):
    """
    All tests relating to the helper functions in utils
    """

    def setUp(self) -> None:
        warnings.filterwarnings("ignore", category=ResourceWarning)
        self.orders = CustomerOrders("unittest_data.xlsx")
        self.dataframe = self.orders.extract_data()

    def test_find_mode_function_returns_expected_value(self) -> None:
        array_sample = ["SEB", "SWEDBANK", "Cash", "Card", "Luminor", "Card"]
        self.assertEqual(find_mode(array_sample), "Card")

    def test_find_mode_returns_first_value_if_all_values_have_same_count(self) -> None:
        example_list = ["Revolut", "SEB", "SWEDBANK", "Cash", "Card", "Luminor"]
        self.assertEqual(find_mode(example_list), "Revolut")

    def test_find_mode_function_does_not_work_on_other_datatypes(self) -> None:
        self.assertRaises(TypeError, find_mode, 100)

    def test_count_if_function_returns_expected_value(self) -> None:
        condition = F.col("discount_active") == 0
        self.assertEqual(
            self.dataframe.select(count_if(condition).alias("no_discount")).collect()[
                0
            ][0],
            2,
        )

    def test_sum_revenue_if_function_returns_expected_value(self) -> None:
        condition = F.col("discount_active") == 0
        self.assertEqual(
            self.dataframe.select(sum_revenue_if(condition).alias("revenue")).collect()[
                0
            ][0],
            58.019999999999996,
        )

    def test_sum_size_if_function_returns_expected_value(self) -> None:
        condition = F.col("order_type") == "Placed"
        self.assertEqual(
            self.dataframe.select(sum_size_if(condition).alias("size")).collect()[0][0],
            34.0,
        )

    def test_sum_profit_if_function_returns_expected_value(self) -> None:
        condition = F.col("order_type") == "Delivered"
        self.assertEqual(
            self.dataframe.select(sum_profit_if(condition).alias("profit")).collect()[
                0
            ][0],
            626.3628000000001,
        )

    def test_count_if_function_returns_expected_value_on_diff_column(self) -> None:
        condition = F.col("week") == 1
        self.assertEqual(
            self.dataframe.select(count_if(condition).alias("first_week")).collect()[0][
                0
            ],
            4,
        )

    def test_sum_revenue_if_function_returns_expected_value_on_diff_column(
        self,
    ) -> None:
        condition = F.col("order_packaging_fee") != 0
        self.assertEqual(
            self.dataframe.select(
                sum_revenue_if(condition).alias("no_packaging_revenue")
            ).collect()[0][0],
            568.74,
        )

    def test_sum_size_if_function_returns_expected_value_on_diff_column(self) -> None:
        condition = F.col("order_packaging_fee") != 0
        self.assertEqual(
            self.dataframe.select(
                sum_size_if(condition).alias("no_packaging_size")
            ).collect()[0][0],
            113.0,
        )

    def test_sum_profit_if_function_returns_expected_value_on_diff_column(self) -> None:
        condition = F.col("order_delivery_fee") > 0
        self.assertAlmostEqual(
            self.dataframe.select(
                sum_profit_if(condition).alias("paid_delivery_fee")
            ).collect()[0][0],
            98.3242,
        )

    def test_mode_function_does_not_work_with_wrong_datatypes(self) -> None:
        self.assertRaises(TypeError, mode, [2, 3, 4, 5, 6, 1])

    def test_mode_function_returns_expected_value(self) -> None:
        self.assertEqual(
            self.dataframe.select(
                mode(F.collect_list("order_type")).alias("top_order_type")
            ).collect()[0][0],
            "Delivered",
        )

    def tearDown(self) -> None:
        del self.dataframe
        del self.orders
