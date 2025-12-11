from pyspark.sql import functions as F

print("=== Data Quality Validation ===\n")

# Load tables
metadata_df = spark.table("lh_aylesbury.metadata_file_tracker")
silver_df = spark.table("lh_aylesbury.silver_transactions")

# Check 1: Row counts
total_rows_expected = metadata_df.agg(F.sum("row_count")).collect()[0][0]
actual_count = silver_df.count()
print(f"Expected rows from metadata: {total_rows_expected}")
print(f"Actual rows in Silver: {actual_count}")

# Check 2: Date range
date_stats = silver_df.select(
    F.min("Date").alias("earliest"),
    F.max("Date").alias("latest"),
    F.countDistinct("Date").alias("unique_dates")
).collect()[0]
print(f"\nDate range: {date_stats.earliest} to {date_stats.latest}")
print(f"Unique dates: {date_stats.unique_dates}")

# Check 3: Null values in critical columns
null_counts = silver_df.select(
    F.count(F.when(F.col("Date").isNull(), 1)).alias("Date_nulls"),
    F.count(F.when(F.col("Amount_Inc_Tax").isNull(), 1)).alias("Amount_nulls"),
    F.count(F.when(F.col("Category").isNull(), 1)).alias("Category_nulls")
).collect()[0]

print(f"\nNull check - Date: {null_counts.Date_nulls}, Amount: {null_counts.Amount_nulls}, Category: {null_counts.Category_nulls}")

# Check 4: Duplicate check
duplicate_count = silver_df.groupBy("Date", "Item", "Amount_Inc_Tax", "Sold_To") \
    .count().filter(F.col("count") > 1).count()
print(f"Duplicate groups found: {duplicate_count}")

# Check 5: Revenue summary
revenue_summary = silver_df.agg(
    F.sum("Amount_Inc_Tax").alias("Total_Revenue"),
    F.avg("Amount_Inc_Tax").alias("Avg_Transaction"),
    F.min("Amount_Inc_Tax").alias("Min_Transaction"),
    F.max("Amount_Inc_Tax").alias("Max_Transaction")
).collect()[0]

print(f"\nRevenue Summary:")
print(f"Total: £{revenue_summary.Total_Revenue:,.2f}")
print(f"Average: £{revenue_summary.Avg_Transaction:,.2f}")
print(f"Range: £{revenue_summary.Min_Transaction:,.2f} - £{revenue_summary.Max_Transaction:,.2f}")

# Check 6: Month_Order verification
incorrect_month_orders = silver_df.filter(
    F.col("Month_Order") != (F.col("Year") * 100 + F.col("Month_Num"))
).count()
print(f"\nIncorrect Month_Order calculations: {incorrect_month_orders}")

# Final validation - fail if critical issues found
critical_failures = []

if duplicate_count > 0:
    critical_failures.append(f"Found {duplicate_count} duplicate transaction groups")

if null_counts.Date_nulls > 0 or null_counts.Amount_nulls > 0 or null_counts.Category_nulls > 0:
    critical_failures.append(f"Null values in critical columns")

if incorrect_month_orders > 0:
    critical_failures.append(f"{incorrect_month_orders} rows have incorrect Month_Order")

if len(critical_failures) > 0:
    print("\n❌ QA FAILED - Critical issues found:")
    for i, failure in enumerate(critical_failures, 1):
        print(f"  {i}. {failure}")
    raise Exception(f"Data Quality Validation Failed: {len(critical_failures)} critical issues")
else:
    print("\n✅ All QA checks passed - data quality validated")
