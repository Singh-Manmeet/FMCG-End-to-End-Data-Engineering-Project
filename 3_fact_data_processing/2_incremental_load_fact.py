# Databricks notebook source
from pyspark.sql import functions as f
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/consolidated_pipeline/utilities

# COMMAND ----------

print(bronze_schema,silver_schema,gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "orders", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f's3://sportsbar-tp-horizon/{data_source}'
landing_path = f'{base_path}/landing/'
processed_path = f'{base_path}/processed/'
print("Base Path: ", base_path)
print("Landing Path: ", landing_path)
print("Processed Path: ", processed_path)


# define the tables
bronze_table = f"{catalog}.{bronze_schema}.{data_source}"
silver_table = f"{catalog}.{silver_schema}.{data_source}"
gold_table = f"{catalog}.{gold_schema}.sb_fact_{data_source}"


# COMMAND ----------

df = spark.read.options(header=True,inferSchema=True).csv(f"{landing_path}/*.csv").withColumn("read_timestamp",f.current_timestamp()).select("*","_metadata.file_name","_metadata.file_size")

display(df)

# COMMAND ----------

df.write \
    .format("delta") \
    .option("delta.enableChangeDatafeed","true") \
    .mode("append") \
    .saveAsTable(f"{bronze_table}")

# COMMAND ----------

df.write \
    .format("delta") \
    .option("delta.enableChangeDatafeed","true") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.{bronze_schema}.staging_{data_source}")

# COMMAND ----------

files = dbutils.fs.ls(landing_path)
for file_info in files:
    dbutils.fs.mv(file_info.path,f"{processed_path}/{file_info.name}",True)

# COMMAND ----------

df_orders = spark.sql(f"select * from {catalog}.{bronze_schema}.staging_{data_source}")
display(df_orders)

# COMMAND ----------

# 1. Keep only rows where order_qty is present
df_orders = df_orders.filter(f.col("order_qty").isNotNull())


# 2. Clean customer_id → keep numeric, else set to 999999
df_orders = df_orders.withColumn(
    "customer_id",
    f.when(f.col("customer_id").rlike("^[0-9]+$"), f.col("customer_id"))
     .otherwise("999999")
     .cast("string")
)

# 3. Remove weekday name from the date text
#    "Tuesday, July 01, 2025" → "July 01, 2025"
df_orders = df_orders.withColumn(
    "order_placement_date",
    f.regexp_replace(f.col("order_placement_date"), r"^[A-Za-z]+,\s*", "")
)

# 4. Parse order_placement_date using multiple possible formats
df_orders = df_orders.withColumn(
    "order_placement_date",
    f.coalesce(
        f.try_to_date("order_placement_date", "yyyy/MM/dd"),
        f.try_to_date("order_placement_date", "dd-MM-yyyy"),
        f.try_to_date("order_placement_date", "dd/MM/yyyy"),
        f.try_to_date("order_placement_date", "MMMM dd, yyyy"),
    )
)

# 5. Drop duplicates
df_orders = df_orders.dropDuplicates(["order_id", "order_placement_date", "customer_id", "product_id", "order_qty"])

# 5. convert product id to string
df_orders = df_orders.withColumn('product_id', f.col('product_id').cast('string'))

# COMMAND ----------



# COMMAND ----------

df_orders.agg(
    f.min("order_placement_date").alias("min_date"),
    f.max("order_placement_date").alias("max_date")
).show()

# COMMAND ----------

df_products = spark.table("fmcg.silver.products")
df_joined = df_orders.join(df_products, on="product_id", how="inner").select(df_orders["*"], df_products["product_code"])

df_joined.show(5)

# COMMAND ----------

if not (spark.catalog.tableExists(silver_table)):
    df_joined.write.format("delta").option(
        "delta.enableChangeDataFeed", "true"
    ).option("mergeSchema", "true").mode("overwrite").saveAsTable(silver_table)
else:
    silver_delta = DeltaTable.forName(spark, silver_table)
    silver_delta.alias("silver").merge(df_joined.alias("bronze"), "silver.order_placement_date = bronze.order_placement_date AND silver.order_id = bronze.order_id AND silver.product_code = bronze.product_code AND silver.customer_id = bronze.customer_id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# stagging for incremental data

df_joined.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{silver_schema}.staging_{data_source}")

# COMMAND ----------

df_gold = spark.sql(f"SELECT order_id, order_placement_date as date, customer_id as customer_code, product_code, product_id, order_qty as sold_quantity FROM {catalog}.{silver_schema}.staging_{data_source};")

df_gold.show(2)

# COMMAND ----------

df_gold.count()

# COMMAND ----------

if not (spark.catalog.tableExists(gold_table)):
    print("creating New Table")
    df_gold.write.format("delta").option(
        "delta.enableChangeDataFeed", "true"
    ).option("mergeSchema", "true").mode("overwrite").saveAsTable(gold_table)
else:
    gold_delta = DeltaTable.forName(spark, gold_table)
    gold_delta.alias("source").merge(df_gold.alias("gold"), "source.date = gold.date AND source.order_id = gold.order_id AND source.product_code = gold.product_code AND source.customer_code = gold.customer_code").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# df_child = your incremental daily rows

df_child =  spark.sql(f"SELECT order_placement_date as date FROM {catalog}.{silver_schema}.staging_{data_source}")

incremental_month_df = df_child.select(
    f.trunc("date", "MM").alias("start_month")
).distinct()

incremental_month_df.show()

incremental_month_df.createOrReplaceTempView("incremental_months")

# COMMAND ----------

monthly_table = spark.sql(f"""
    SELECT date, product_code, customer_code, sold_quantity
    FROM {catalog}.{gold_schema}.sb_fact_orders sbf
    INNER JOIN incremental_months m
        ON trunc(sbf.date, 'MM') = m.start_month
""")

print("Total Rows: ", monthly_table.count())
monthly_table.show(10)

# COMMAND ----------

monthly_table.select('date').distinct().orderBy('date').show()

# COMMAND ----------

df_monthly_recalc = (
    monthly_table
    .withColumn("month_start", f.trunc("date", "MM"))
    .groupBy("month_start", "product_code", "customer_code")
    .agg(f.sum("sold_quantity").alias("sold_quantity"))
    .withColumnRenamed("month_start", "date")   # month_start → date = first of month
)

df_monthly_recalc.show(10, truncate=False)

# COMMAND ----------

df_monthly_recalc.count()

# COMMAND ----------

gold_parent_delta = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.fact_orders")
gold_parent_delta.alias("parent_gold").merge(df_monthly_recalc.alias("child_gold"), "parent_gold.date = child_gold.date AND parent_gold.product_code = child_gold.product_code AND parent_gold.customer_code = child_gold.customer_code").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table fmcg.bronze.staging_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table fmcg.silver.staging_orders;

# COMMAND ----------

# MAGIC %md
# MAGIC