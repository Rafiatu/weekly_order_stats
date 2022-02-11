import logging
import os
from src.processor import CustomerOrders
from unittest import TestCase
import warnings


logging.disable(logging.CRITICAL)


class TestDataPipeline(TestCase):
    """
    All tests relating to the main ETL functions for transforming customer daily orders to weekly orders.
    """

    def setUp(self) -> None:
        warnings.filterwarnings("ignore", category=ResourceWarning)
        self.pipeline = CustomerOrders("unittest_data.xlsx")
        self.error_pipeline = CustomerOrders("wrong_location.csv")

    def test_data_is_extracted_as_expected(self) -> None:
        self.assertEqual(
            self.pipeline.extract_data().columns,
            [
                "customer_id",
                "order_id",
                "order_type",
                "order_date",
                "order_city",
                "order_shop",
                "order_source",
                "order_revenue",
                "order_size",
                "order_cost",
                "order_profit",
                "order_packaging_fee",
                "order_delivery_fee",
                "discount_active",
                "discount_amount",
                "address_id",
                "payment_source",
                "days_since_last_order",
                "first_order_active",
                "dt",
                "week",
            ],
        )

    def test_extract_data_function_handles_errors_properly(self) -> None:
        self.assertRaises(FileNotFoundError, self.error_pipeline.extract_data)

    def test_transform_data_function_handles_errors_properly(self) -> None:
        self.assertRaises(AttributeError, self.error_pipeline.transform_data)

    def test_data_is_aggregated_and_properly_transformed(self) -> None:
        self.pipeline.extract_data()
        self.assertEqual(
            self.pipeline.transform_data().columns,
            [
                "customer_id",
                "order_executed_count",
                "order_placed_count",
                "order_returned_count",
                "order_size_avg",
                "order_size_executed_sum",
                "order_size_placed_sum",
                "order_size_returned_sum",
                "order_revenue_sum",
                "order_revenue_avg",
                "order_revenue_executed_sum",
                "order_revenue_placed_sum",
                "order_revenue_returned_sum",
                "order_profit_sum",
                "order_profit_avg",
                "order_profit_executed_sum",
                "order_profit_placed_sum",
                "order_profit_returned_sum",
                "discount_active",
                "discount_amount_avg",
                "last_address",
                "top_address",
                "payment_source_top",
                "delivery_fee_avg",
                "order_w_delivery_fee",
                "packaging_fee_avg",
                "order_w_packaging_fee",
                "dt",
            ],
        )

    def test_saves_appropriate_data_into_csv_file(self) -> None:
        self.pipeline.extract_data()
        self.pipeline.transform_data()
        storage_path = "output/test_data.csv"
        self.assertTrue(self.pipeline.load_data_to_csv(storage_path))
        self.assertTrue(os.path.exists(storage_path))

    def test_load_data_to_csv_does_not_save_to_any_other_file_format(self):
        self.assertRaises(ValueError, self.pipeline.load_data_to_csv, "excel_file.xlsx")

    def tearDown(self) -> None:
        if os.path.exists("output/test_data.csv"):
            os.remove("output/test_data.csv")
        del self.pipeline
        del self.error_pipeline
