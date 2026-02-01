"""
Example 3: Unnecessary Aliases in Joins

Problem: Excessive use of aliases makes the code harder to read and maintain.
The original developer used aliases even when they weren't necessary for disambiguation.
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

        # Overly complex join with unnecessary aliases
        supersede = supersede_product.alias("sup").join(
            product.alias("pm1"),
            supersede_product["product_id"] == product["product_id"],
            "left"
        ).select(
            F.col("sup.family_id"),
            F.col("pm1.product_code"),
            F.col("pm1.product_name")
        )

        # Another unnecessarily aliased join
        supersede = supersede.alias("s").join(
            category.alias("cat"),
            F.col("s.family_id") == F.col("cat.family_id"),
            "left"
        ).select(
            F.col("s.product_code"),
            F.col("s.product_name"),
            F.col("cat.category_name")
        )

        return supersede
