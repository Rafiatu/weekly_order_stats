from src.processor import CustomerOrders


data = CustomerOrders("unittest_data.xlsx")
print(data.extract_data())
data.transform_data()
data.load_data_to_csv("output/weekly_customer_orders.csv")
