"""
Example 1: Unused Columns (Refactored)

Solution: Removed the unused `location_type` and `priority_level` columns.
Only columns that appear in the final select are computed.
"""

from pyspark.sql import functions as F


class PreETLScope:
    def __init__(self, run_date):
        self.run_date = run_date

    def main(self, replenishment_scope):
        # Adding only the columns that will be used (no chaining)
        replenishment_scope = replenishment_scope.withColumn("start_date", F.lit(self.run_date))
        replenishment_scope = replenishment_scope.withColumn("solution_id", F.lit(21))

        replenishment_scope = replenishment_scope.select("start_date", "solution_id")

        return replenishment_scope
