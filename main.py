from src.processor import CustomerOrders


orders = CustomerOrders("unittest_data.xlsx")
print(orders.extract_data())
orders.transform_data()
orders.load_data_to_csv("output/weekly_customer_orders.csv")
