"""
Example 3: Unnecessary Aliases in Joins (Refactored)

Solution: Removed unnecessary aliases and used direct column references.
The join syntax is simplified using the `on=` parameter where possible.
"""

from pyspark.sql import functions as F


class PreETLSupersede:
    def __init__(self, spark):
        self.spark = spark

    def read_input(self, name):
        return self.spark.table(name)

    def main(self):
        # Data Ingestion
        supersede_product = self.read_input("supersede_product")
        product = self.read_input("product")
        category = self.read_input("category")

        # Simplified join - no aliases, no chaining
        supersede = supersede_product.join(product, on="product_id", how="left")
        supersede = supersede.select("family_id", "product_code", "product_name")

        # Second join - also simplified
        supersede = supersede.join(category, on="family_id", how="left")
        supersede = supersede.select("product_code", "product_name", "category_name")

        return supersede
