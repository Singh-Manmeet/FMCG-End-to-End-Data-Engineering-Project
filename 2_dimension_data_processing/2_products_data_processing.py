# Databricks notebook source
from pyspark.sql import functions as f
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/consolidated_pipeline/utilities

# COMMAND ----------

print(bronze_schema,silver_schema,gold_schema)

# COMMAND ----------

dbutils.widgets.text('catalog','fmcg','catalog')
dbutils.widgets.text('data_source','customers','Data Source')

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f"s3://sportsbar-tp-horizon/{data_source}/*.csv"
print(base_path)

# COMMAND ----------

df = spark.read \
    .format("csv") \
    .option("header","true") \
    .option("inferSchema","true") \
    .load(base_path) \
    .withColumn("read_timestamp",f.current_timestamp()) \
    .select("*","_metadata.file_name","_metadata.file_size")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

df.write \
    .format("delta") \
    .option("delta.enableChangeDataFeed","true") \
    .option("mergeSchema","true") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")

# COMMAND ----------

df_silver = spark.sql(f"select * from {catalog}.{bronze_schema}.{data_source}")
display(df_silver)

# COMMAND ----------

display(df_silver.groupBy("product_id").count().filter(f.col("count")>1))

# COMMAND ----------

print("row count before drop ", df_silver.count())
df_silver = df_silver.dropDuplicates(['product_id'])
print("row count after drop ", df_silver.count())


# COMMAND ----------

df_silver.select("category").distinct().show()

# COMMAND ----------

df_silver = df_silver.withColumn("category",
                     f.when(f.col("category").isNull(),None)
                     .otherwise(f.initcap(f.col("category")))
                     )

# COMMAND ----------

display(df_silver)

# COMMAND ----------

df_silver = df_silver.withColumn("product_name",f.regexp_replace(f.col("product_name"),"(?i)protien","Protein"))

# COMMAND ----------

display(df_silver)

# COMMAND ----------

df_silver = df_silver.withColumn("category",f.regexp_replace(f.col("category"),"(?i)protien","Protein"))

# COMMAND ----------

display(df_silver)

# COMMAND ----------

df_silver = (
    df_silver
    .withColumn(
        "division",
        f.when(f.col("category") == "Energy Bars", "Nutrition Bars")
        .when(f.col("category") == "Protein Bars", "Nutrition Bars")
        .when(f.col("category") == "Granola & Cereals", "Breakfast Foods")
        .when(f.col("category") == "Recovery Dairy", "Dairy & Recovery")
        .when(f.col("category") == "Healthy Snacks", "Healthy Snacks")
        .when(f.col("category") == "Electrolyte Mix", "Hydration & Electrolytes")
        .otherwise("Other")
    )
)

df_silver = df_silver.withColumn(
    "variant",
    f.regexp_extract(f.col("product_name"), r"\((.*?)\)", 1)
)

df_silver = (
    df_silver
    .withColumn("product_code",
    f.sha2(f.col("product_name").cast("string"),256))
    .withColumn("product_id",
        f.when(
            f.col("product_id").cast("string").rlike("^[0-9]+$"),
            f.col("product_id").cast("string")
        ).otherwise(f.lit(999999).cast("string"))
    )
)


# COMMAND ----------

display(df_silver)

# COMMAND ----------

df_silver = df_silver.withColumnRenamed("product_name","product")
df_silver.printSchema()

# COMMAND ----------

df_silver = df_silver.select("product_code","division","category","product","variant","product_id","read_timestamp","file_name","file_size")

# COMMAND ----------

display(df_silver)

# COMMAND ----------

df_silver.write \
    .format("delta") \
    .option("delta.enableChangeDataFeed","true") \
    .option("mergeSchema","true") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

df_silver = spark.sql("select * from fmcg.silver.products")
df_gold = df_silver.select("product_code","product_id","division","category","product","variant")
df_gold.show()

# COMMAND ----------

df_gold.write \
    .format("delta") \
    .option("delta.enableChangeDataFeed","true") \
    .mode("overwrite") \
    .option("mergeSchema","true") \
    .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")

# COMMAND ----------

delta_table = DeltaTable.forName(spark,"fmcg.gold.dim_products")
df_child_products = spark.sql(f"select * from fmcg.gold.sb_dim_products")
df_child_products.show(20)

# COMMAND ----------

display(df_child_products)

# COMMAND ----------

from delta.tables import DeltaTable

delta_table.alias("target").merge(
    source=df_child_products.alias("source"),
    condition="source.product_code=target.product_code"
).whenMatchedUpdate(
    set={
        "division":"source.division",
        "category":"source.category",
        "product":"source.product",
        "variant":"source.variant"
    }
).whenNotMatchedInsert(
    values={
        "product_code":"source.product_code",
        "division":"source.division",
        "category":"source.category",
        "product":"source.product",
        "variant":"source.variant",
    }
).execute()    

# COMMAND ----------

