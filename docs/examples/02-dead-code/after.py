"""
Example 2: Dead Code Blocks (Refactored)

Solution: Removed the unused `season_list`, `replen_items_seasonal`, 
`seasonal_summary`, and `location_metrics` definitions.
Also removed the unused `locations` read since it's not needed.
"""

from pyspark.sql import functions as F


class PreETLInventory:
    def __init__(self, spark):
        self.spark = spark

    def read_input(self, name):
        # Simulated read
        return self.spark.table(name)

    def main(self):
        # Data Ingestion - only tables that contribute to output
        orders = self.read_input("orders")
        products = self.read_input("products")

        # Processing - clean, linear, no chaining
        result = orders.join(products, on="product_id", how="left")
        result = result.select("order_id", "product_name", "quantity")

        return result
