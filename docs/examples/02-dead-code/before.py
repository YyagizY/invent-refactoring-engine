"""
Example 2: Dead Code Blocks

Problem: `season_list` and `replen_items_seasonal` are defined but never used
in any downstream logic. This is dead code that adds confusion and maintenance burden.
"""

from pyspark.sql import functions as F


class PreETLInventory:
    def __init__(self, spark):
        self.spark = spark

    def read_input(self, name):
        # Simulated read
        return self.spark.table(name)

    def main(self):
        # Data Ingestion
        orders = self.read_input("orders")
        products = self.read_input("products")
        locations = self.read_input("locations")

        # Dead code block - these are never used in the final output!
        season_list = [1, 2, 3, 4]
        replen_items_seasonal = orders.filter(F.col("season").isin(season_list))
        seasonal_summary = replen_items_seasonal.groupBy("season").count()

        # Another dead code block - location_metrics is never used
        location_metrics = (
            locations
            .groupBy("region")
            .agg(F.count("*").alias("location_count"))
        )

        # This is the only code that contributes to the output (but uses chaining)
        result = (
            orders
            .join(products, on="product_id", how="left")
            .select("order_id", "product_name", "quantity")
        )

        return result
