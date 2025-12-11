from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, StringType, TimestampType, StructType, StructField
from datetime import datetime
from notebookutils import mssparkutils

print("=== Bronze to Silver - Incremental Loading ===\n")

# Initialize metadata table if it doesn't exist
try:
    metadata_df = spark.table("lh_aylesbury.metadata_file_tracker")
    print(f"Metadata table exists: {metadata_df.count()} records")
except:
    metadata_schema = StructType([
        StructField("file_name", StringType(), True),
        StructField("file_path", StringType(), True),
        StructField("processed_date", TimestampType(), True),
        StructField("row_count", IntegerType(), True)
    ])
    empty_df = spark.createDataFrame([], metadata_schema)
    empty_df.write.format("delta").mode("overwrite").saveAsTable("lh_aylesbury.metadata_file_tracker")
    print("Metadata table created")

# Scan Bronze folder for CSV files
all_files = mssparkutils.fs.ls("Files/Bronze/")
csv_files = [f for f in all_files if f.name.endswith('.csv')]
print(f"\nFound {len(csv_files)} CSV files in Bronze")

# Identify new files to process
metadata_df = spark.table("lh_aylesbury.metadata_file_tracker")
processed_files = [row.file_name for row in metadata_df.select("file_name").collect()]
new_files = [f for f in csv_files if f.name not in processed_files]

if len(new_files) == 0:
    print("No new files to process - pipeline complete")
else:
    print(f"Processing {len(new_files)} new files\n")
    
    all_dataframes = []
    file_counts = {}

    for f in new_files:
        df_file = spark.read.format("csv").option("header", "true").option("inferSchema", "false").load(f.path)
        file_row_count = df_file.count()
        file_counts[f.name] = file_row_count
        print(f"Loaded {f.name}: {file_row_count} rows")
        all_dataframes.append(df_file)

    df_raw = all_dataframes[0]
    for df in all_dataframes[1:]:
        df_raw = df_raw.union(df)

    print(f"\nTotal new records: {df_raw.count()}")

    # Clean and transform
    df_clean = df_raw \
        .withColumn("Date", F.to_date(F.col("Date").substr(1, 10), "dd/MM/yyyy")) \
        .withColumnRenamed("Quantity Sold", "Quantity_Sold") \
        .withColumnRenamed("Amount Inc Tax", "Amount_Inc_Tax") \
        .withColumnRenamed("Sold By", "Sold_By") \
        .withColumnRenamed("Sold To", "Sold_To") \
        .drop("Tax Rate", "Type", "Late Payment") \
        .withColumn("Quantity_Sold", F.col("Quantity_Sold").cast(IntegerType())) \
        .withColumn("Amount_Inc_Tax", F.col("Amount_Inc_Tax").cast(DoubleType()))

    # Add date features
    df_silver = df_clean \
        .withColumn("Year", F.year(F.col("Date"))) \
        .withColumn("Month_Num", F.month(F.col("Date"))) \
        .withColumn("Month_Name", F.date_format(F.col("Date"), "MMMM")) \
        .withColumn("Month_Abbrev", F.date_format(F.col("Date"), "MMM")) \
        .withColumn("Month_Year", F.concat(F.date_format(F.col("Date"), "MMM"), F.lit(" "), F.year(F.col("Date")))) \
        .withColumn("Quarter", F.concat(F.lit("Q"), F.quarter(F.col("Date")))) \
        .withColumn("Month_Order", F.year(F.col("Date")) * 100 + F.month(F.col("Date"))) \
        .withColumn("processed_timestamp", F.lit(datetime.now()))

    # Deduplicate
    rows_before = df_silver.count()
    df_silver = df_silver.dropDuplicates(["Date", "Item", "Amount_Inc_Tax", "Sold_To", "Quantity_Sold", "Category", "Sold_By"])
    rows_after = df_silver.count()
    
    print(f"\nRemoved {rows_before - rows_after} duplicates")
    print(f"Loading {rows_after} unique records to Silver")

    # Write to Silver table
    df_silver.write.format("delta").mode("append").saveAsTable("lh_aylesbury.silver_transactions")

    # Update metadata tracker
    metadata_schema = StructType([
        StructField("file_name", StringType(), True),
        StructField("file_path", StringType(), True),
        StructField("processed_date", TimestampType(), True),
        StructField("row_count", IntegerType(), True)
    ])

    for f in new_files:
        new_metadata = spark.createDataFrame([
            (f.name, f.path, datetime.now(), int(file_counts.get(f.name, 0)))
        ], metadata_schema)
        new_metadata.write.format("delta").mode("append").saveAsTable("lh_aylesbury.metadata_file_tracker")

    print(f"\n✅ Pipeline complete: {len(new_files)} files processed, {rows_after} records added")
