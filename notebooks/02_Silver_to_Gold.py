from pyspark.sql import functions as F
from pyspark.sql.window import Window

print("=== Silver to Gold - Business Aggregations ===\n")

df_silver = spark.table("lh_aylesbury.silver_transactions")
print(f"Silver table loaded: {df_silver.count()} records")

# Gold Table 1: Monthly Summary
print("\nCreating gold_monthly_summary...")
gold_monthly = df_silver.groupBy(
    "Year", "Month_Num", "Month_Name", "Month_Abbrev", "Month_Year", "Quarter", "Month_Order"
).agg(
    F.round(F.sum("Amount_Inc_Tax"), 1).alias("Total_Revenue"),
    F.sum("Quantity_Sold").alias("Total_Units"),
    F.count("*").alias("Transaction_Count"),
    F.countDistinct("Sold_To").alias("Unique_Customers"),
    F.round(F.avg("Amount_Inc_Tax"), 1).alias("Avg_Transaction_Value")
).withColumn("Month_Short", 
    F.concat(
        F.lpad(F.col("Month_Num"), 2, "0"),
        F.lit("/"),
        F.substring(F.col("Year").cast("string"), 3, 2)
    )
)

gold_monthly.write.format("delta").mode("overwrite").saveAsTable("lh_aylesbury.gold_monthly_summary")
print(f"✓ gold_monthly_summary: {gold_monthly.count()} months")

# Gold Table 2: Yearly Summary
print("\nCreating gold_yearly_summary...")
gold_yearly = df_silver.groupBy("Year").agg(
    F.round(F.sum("Amount_Inc_Tax"), 1).alias("Total_Revenue"),
    F.sum("Quantity_Sold").alias("Total_Units"),
    F.count("*").alias("Transaction_Count"),
    F.countDistinct("Sold_To").alias("Unique_Customers"),
    F.round(F.avg("Amount_Inc_Tax"), 1).alias("Avg_Transaction_Value")
)

gold_yearly.write.format("delta").mode("overwrite").saveAsTable("lh_aylesbury.gold_yearly_summary")
print(f"✓ gold_yearly_summary: {gold_yearly.count()} years")

# Gold Table 3: Category Summary by Year
print("\nCreating gold_category_summary...")
window_spec = Window.partitionBy("Year")
gold_category = df_silver.groupBy("Year", "Category").agg(
    F.round(F.sum("Amount_Inc_Tax"), 1).alias("Total_Revenue"),
    F.sum("Quantity_Sold").alias("Total_Units"),
    F.count("*").alias("Transaction_Count")
).withColumn(
    "Revenue_Percentage",
    F.round((F.col("Total_Revenue") / F.sum("Total_Revenue").over(window_spec)) * 100, 1)
)

gold_category.write.format("delta").mode("overwrite").saveAsTable("lh_aylesbury.gold_category_summary")
print(f"✓ gold_category_summary: {gold_category.count()} category-year combinations")

# Gold Table 4: Quarterly Summary
print("\nCreating gold_quarterly_summary...")
gold_quarterly = df_silver.groupBy("Year", "Quarter").agg(
    F.round(F.sum("Amount_Inc_Tax"), 1).alias("Total_Revenue"),
    F.sum("Quantity_Sold").alias("Total_Units"),
    F.count("*").alias("Transaction_Count"),
    F.countDistinct("Sold_To").alias("Unique_Customers"),
    F.round(F.avg("Amount_Inc_Tax"), 1).alias("Avg_Transaction_Value")
).withColumn("Quarter_Display", F.concat(F.col("Quarter"), F.lit(" "), F.col("Year"))) \
 .withColumn("Quarter_Order", F.col("Year") * 10 + F.substring(F.col("Quarter"), 2, 1).cast("int"))

gold_quarterly.write.format("delta").mode("overwrite").saveAsTable("lh_aylesbury.gold_quarterly_summary")
print(f"✓ gold_quarterly_summary: {gold_quarterly.count()} quarters")

# Gold Table 5: Item Performance by Year
print("\nCreating gold_item_performance...")
gold_items = df_silver.groupBy("Year", "Item", "Category").agg(
    F.round(F.sum("Amount_Inc_Tax"), 1).alias("Total_Revenue"),
    F.sum("Quantity_Sold").alias("Total_Units"),
    F.count("*").alias("Transaction_Count")
)

gold_items.write.format("delta").mode("overwrite").saveAsTable("lh_aylesbury.gold_item_performance")
print(f"✓ gold_item_performance: {gold_items.count()} item-year combinations")

print("\n✅ All Gold tables created successfully")
