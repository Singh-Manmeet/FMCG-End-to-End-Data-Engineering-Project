# Databricks notebook source
from pyspark.sql import functions as f
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/consolidated_pipeline/utilities

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
    .option("header",True) \
    .option("inferSchema",True) \
    .load(base_path) \
    .withColumn("read_timestamp",f.current_timestamp()) \
    .select("*","_metadata.file_name","_metadata.file_size")

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.write \
    .format("delta") \
    .option("delta.enableChangeDataFeed","true") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")

# COMMAND ----------

df_bronze = spark.sql(f"select * from {catalog}.{bronze_schema}.{data_source};")
df_bronze.show(20)

# COMMAND ----------

df_bronze.printSchema()

# COMMAND ----------

df_bronze.groupBy("customer_id").count().filter(f.col("count")>1).select("*").show()

# COMMAND ----------

print("row count before ",df_bronze.count())
df_silver = df_bronze.dropDuplicates(['customer_id'])
print("row count after ",df_silver.count())

# COMMAND ----------

display(df_silver.filter(f.col("customer_name") != f.trim(f.col("customer_name"))))

# COMMAND ----------

df_silver = df_silver.withColumn("customer_name",f.trim(f.col("customer_name")))

# COMMAND ----------

display(df_silver)

# COMMAND ----------

display(df_silver.filter(f.col("customer_name") != f.trim(f.col("customer_name"))))

# COMMAND ----------

display( df_silver.select("city").distinct())

# COMMAND ----------

city_mapping = {
    'Bengaluruu': 'Bengaluru',
    'Bangalore': 'Bengaluru',
    
    'Hyderabadd': 'Hyderabad',
    'Hyderbad': 'Hyderabad',
    
    'NewDelhi': 'New Delhi',
    'NewDheli': 'New Delhi',
    'NewDelhee': 'New Delhi'
}


allowed = ["Bengaluru", "Hyderabad", "New Delhi"]

df_silver = (
    df_silver
    .replace(city_mapping, subset=["city"])
    .withColumn(
        "city",
        f.when(f.col("city").isNull(), None)
        .when(f.col("city").isin(allowed), f.col("city"))
        .otherwise(None)
    )
)



df_silver.select("city").distinct().show()



# COMMAND ----------

df_silver.select("customer_name").distinct().show()

# COMMAND ----------

df_silver = df_silver.withColumn("customer_name",f.when(f.col("customer_name").isNull(),None)
                                 .otherwise(f.initcap("customer_name")))

df_silver.select("customer_name").distinct().show()

# COMMAND ----------

df_silver.filter(f.col("city").isNull()).show(truncate=False)   

# COMMAND ----------

null_cities_customer = ["Endurance Foods","Sprintx Nutrition","Zenathlete Foods","Primefuel Nutrition","Recovery Lane"]



df_silver.filter(f.col("customer_name").isin(null_cities_customer)).select("customer_name","city").show()

# COMMAND ----------

customer_city_fix = {
    # Sprintx Nutrition
    789403: "New Delhi",

    789101: "New Delhi",
    
    # Zenathlete Foods
    789420: "Bengaluru",
    
    # Primefuel Nutrition
    789521: "Hyderabad",
    
    # Recovery Lane
    789603: "Hyderabad"
}

city_fix = spark.createDataFrame([(k,v) for k,v in customer_city_fix.items()],
                      ["customer_id","fixed_city"])


city_fix.show()







# COMMAND ----------

df_silver = df_silver.join(city_fix,on="customer_id",how='left').withColumn("city",f.coalesce("city",'fixed_city')).drop("fixed_city")



df_silver.filter(f.col("customer_id").isin(list(customer_city_fix))).select("*").show()

# COMMAND ----------

df_silver.filter(f.col("customer_name").isin(null_cities_customer)).select("customer_name","city").show()

# COMMAND ----------

df_silver.printSchema()

# COMMAND ----------

df_silver = df_silver.withColumn("customer_id",f.col("customer_id").cast("string"))
df_silver.printSchema()

# COMMAND ----------

df_silver = df_silver.withColumn("customer",f.concat_ws("-",f.col("customer_name"),f.col("city") )) \
.withColumn("market",f.lit("India")) \
.withColumn("platform",f.lit("sportsbar")) \
.withColumn("channel",f.lit("acquisition"))

display(df_silver)

# COMMAND ----------

df_silver.write \
    .format("delta") \
    .option("delta.enableChangeDataFeed","true") \
    .option("mergeSchema","true") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Gold processing

# COMMAND ----------

df_silver = spark.sql(f"select * from {catalog}.{silver_schema}.{data_source}")



display(df_silver)

# COMMAND ----------

df_gold = df_silver.select("customer_id","customer_name","city","customer","market","platform","channel")

# COMMAND ----------

df_gold.write \
    .format("delta") \
    .option("enableChangeDataFeed","true") \
    .option("mergeSchema","true") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")

# COMMAND ----------

from pyspark.sql import functions as f
from delta.tables import DeltaTable


delta_table = DeltaTable.forName(spark,"fmcg.gold.dim_customers")
df_child_customers = spark.table("fmcg.gold.sb_dim_customers") \
    .select(
        f.col("customer_id").alias("customer_code"),
        "customer",
        "market",
        "platform",
        "channel"
    )    


# COMMAND ----------

df_child_customers.printSchema()

# COMMAND ----------

delta_table.alias("target").merge(
    source=df_child_customers.alias("source"),
    condition="target.customer_code=source.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()