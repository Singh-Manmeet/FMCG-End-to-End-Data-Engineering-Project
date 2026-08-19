# Databricks notebook source
from pyspark.sql import functions as f
from delta.tables import DeltaTable
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run /Workspace/consolidated_pipeline/utilities

# COMMAND ----------

print(bronze_schema,silver_schema,gold_schema)

# COMMAND ----------

dbutils.widgets.text('catalog','fmcg','catalog')
dbutils.widgets.text('data_source','customers','Data Source')


catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f"s3://sportsbar-tp-horizon/{data_source}/*.csv"
print(base_path)

# COMMAND ----------

df = (
    spark.read.format("csv")
    .option("header","true")
    .option("inferSchema","true")
    .load(base_path)
    .withColumn("read_timestamp",f.current_timestamp())
    .select("*","_metadata.file_name","_metadata.file_size")
)

# COMMAND ----------

display(df)

# COMMAND ----------

df.write \
.format("delta") \
.option("delta.enableChangeDataFeed","true") \
.mode("overwrite") \
.saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")

# COMMAND ----------

df_bronze = spark.sql(f"select * from fmcg.bronze.gross_price")
display(df_bronze)

# COMMAND ----------

display(df_bronze.select("month").distinct())

# COMMAND ----------

df_silver = df_bronze.withColumn(
    "month",
    f.coalesce(
        f.try_to_date(f.col("month"), "yyyy/MM/dd"), # NULL
        f.try_to_date(f.col("month"), "dd/MM/yyyy"), # NULL
        f.try_to_date(f.col("month"), "yyyy-MM-dd"), # valid date
        f.try_to_date(f.col("month"), "dd-MM-yyyy")  # NULL
    )
)

# COMMAND ----------

display(df_silver.select("month").distinct())

# COMMAND ----------

df_silver = df_silver.withColumn(
    "gross_price",
    f.when(f.col("gross_price").rlike(r'^-?\d+(\.\d+)?$'),
        f.when(f.col("gross_price").cast("double") < 0, -1 * f.col("gross_price").cast("double"))
        .otherwise(f.col("gross_price").cast("double"))
    )
    .otherwise(0)
)

# COMMAND ----------

display(df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

df_products = spark.table("fmcg.silver.products")
df_joined = df_silver.join(df_products.select("product_id","product_code"),on="product_id",how="inner")
df_joined = df_joined.select("product_id","product_code","month","gross_price","read_timestamp","file_name","file_size")
display(df_joined)

# COMMAND ----------



df_joined.write \
    .format("delta") \
    .option("delta.enableChangeDataFeed","true") \
    .option("mergeSchema","true") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")



# COMMAND ----------

df_silver = spark.table("fmcg.silver.gross_price")
display(df_silver)

# COMMAND ----------

df_gold = df_silver.select("product_code","month","gross_price")

# COMMAND ----------

display(df_gold)

# COMMAND ----------

display(df_gold)

# COMMAND ----------

df_gold.write \
    .format("delta") \
    .option("delta.enableChangeDataFeed","true") \
    .option("mergeSchema","true") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")

# COMMAND ----------

df_gold_price = spark.table("fmcg.gold.sb_dim_gross_price")
display(df_gold_price)

# COMMAND ----------

from pyspark.sql import functions as f
from delta.tables import DeltaTable
from pyspark.sql.window import Window


df_gold_price = (
    df_gold_price
    .withColumn("year", f.year("month"))
    # 0 = non-zero price, 1 = zero price -> non-zero comes first
    .withColumn("is_zero", f.when(f.col("gross_price") == 0, 1).otherwise(0))
)

w = (
    Window
    .partitionBy("product_code", "year")
    .orderBy(f.col("is_zero"), f.col("month").desc())
)

df_gold_latest_price = (
    df_gold_price
    .withColumn("rnk", f.row_number().over(w))
    .filter(f.col("rnk") == 1)
)

# COMMAND ----------

display(df_gold_latest_price)

# COMMAND ----------

df_gold_latest_price = (
    df_gold_latest_price.select("product_code","year","gross_price")
.withColumnRenamed("gross_price","price_inr").select("product_code","price_inr","year")
)

df_gold_latest_price = (
    df_gold_latest_price.withColumn("year",f.col("year").cast("string"))
)

df_gold_latest_price.show(5)

# COMMAND ----------

from pyspark.sql import functions as f
from delta.tables import DeltaTable
from pyspark.sql.window import Window

delta_table = DeltaTable.forName(spark,"fmcg.gold.dim_gross_price")


delta_table.alias("target").merge(
    source=df_gold_latest_price.alias("source"),
    condition="source.product_code=target.product_code"
).whenMatchedUpdate(
    set={
        "target.price_inr":"source.price_inr",
        "target.year":"source.year",
    }
).whenNotMatchedInsert(
    values={
        "product_code":"source.product_code",
        "price_inr":"source.price_inr",
        "year":"source.year"
    }
).execute()




# COMMAND ----------

df = spark.table("fmcg.gold.dim_gross_price")
df.count()

# COMMAND ----------

display(df)

# COMMAND ----------

