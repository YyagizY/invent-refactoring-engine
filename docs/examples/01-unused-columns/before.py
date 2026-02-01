"""
Example 1: Unused Columns

Problem: The `location_type` column is calculated but never selected in the final output.
This adds unnecessary computation and makes the code harder to understand.
"""

from pyspark.sql import functions as F


class PreETLScope:
    def __init__(self, run_date):
        self.run_date = run_date

    def main(self, replenishment_scope):
        # Adding multiple columns (chained - also bad practice)
        replenishment_scope = (
            replenishment_scope
            .withColumn("start_date", F.lit(self.run_date))
            .withColumn("solution_id", F.lit(21))
            .withColumn("location_type", F.lit(0))  # This column is never used!
            .withColumn("priority_level", F.lit("HIGH"))  # This column is never used!
        )

        # Only start_date and solution_id are selected
        replenishment_scope = replenishment_scope.select("start_date", "solution_id")

        return replenishment_scope
