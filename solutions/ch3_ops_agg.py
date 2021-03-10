# %%
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ch3_rows").getOrCreate()

# %%

from pyspark.sql.types import *

fire_schema = StructType(
    [
        StructField("CallNumber", IntegerType(), True),
        StructField("UnitID", StringType(), True),
        StructField("IncidentNumber", IntegerType(), True),
        StructField("CallType", StringType(), True),
        StructField("CallDate", StringType(), True),
        StructField("WatchDate", StringType(), True),
        StructField("CallFinalDisposition", StringType(), True),
        StructField("AvailableDtTm", StringType(), True),
        StructField("Address", StringType(), True),
        StructField("City", StringType(), True),
        StructField("Zipcode", IntegerType(), True),
        StructField("Battalion", StringType(), True),
        StructField("StationArea", StringType(), True),
        StructField("Box", StringType(), True),
        StructField("OriginalPriority", StringType(), True),
        StructField("Priority", StringType(), True),
        StructField("FinalPriority", IntegerType(), True),
        StructField("ALSUnit", BooleanType(), True),
        StructField("CallTypeGroup", StringType(), True),
        StructField("NumAlarms", IntegerType(), True),
        StructField("UnitType", StringType(), True),
        StructField("UnitSequenceInCallDispatch", IntegerType(), True),
        StructField("FirePreventionDistrict", StringType(), True),
        StructField("SupervisorDistrict", StringType(), True),
        StructField("Neighborhood", StringType(), True),
        StructField("Location", StringType(), True),
        StructField("RowID", StringType(), True),
        StructField("Delay", FloatType(), True),
    ]
)

sf_fire_file = (
    "../LearningSparkV2/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv"
)
fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)

fire_df

# %%

parquet_path = "./fire_df.parquet"
fire_df.write.format("parquet").save(parquet_path)

# %%

import pyspark.sql.functions as F

few_fire_df = (
    fire_df.select("IncidentNumber", "AvailableDtTm", "CallType")
    .where(F.col("CallType") != "Medical Incident")
    .show(5)
)

# %% How many distinct call types

len(fire_df.select("CallType").groupBy("CallType").count().collect())

# %% How many distinct call types (SQL like)

fire_df.select("CallType").agg(
    F.countDistinct("CallType").alias("DistinctCallTypes")
).show()

fire_df.select("CallType").groupBy("CallType").agg(
    F.countDistinct("CallType").alias("DistinctCallTypes")
).count()

fire_df.select("CallType").distinct().count()

# %%

new_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayedinMins")

(
    new_fire_df.select("ResponseDelayedinMins")
    .where(F.col("ResponseDelayedinMins") > 5)
    .show(5, False)
)


# %%

fire_ts_df = (
    new_fire_df.withColumn(
        "IncidentDate", F.to_timestamp(F.col("CallDate"), "MM/dd/yyyy")
    )
    .drop("CallDate")
    .withColumn("OnWatchDate", F.to_timestamp(F.col("WatchDate"), "MM/dd/yyyy"))
    .drop("WatchDate")
    .withColumn(
        "AvailableDtTS", F.to_timestamp(F.col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a")
    )
    .drop("AvailableDtTm")
)

new_fire_df.select("CallDate", "WatchDate", "AvailableDtTm").show(5, False)

fire_ts_df.select("IncidentDate", "OnWatchDate", "AvailableDtTS").show(5, False)

fire_ts_df.select("IncidentDate", "OnWatchDate", "AvailableDtTS").schema

# %%

(
    fire_ts_df.select(F.year("IncidentDate"))
    .distinct()
    .orderBy(F.year("IncidentDate"))
    .show()
)

# %% What were the most common types of fire calls?

(
    fire_ts_df.select("CallType")
    .where(F.col("CallType").isNotNull())
    .groupBy("CallType")
    .count()
    .orderBy("count", ascending=False)
    .show()
)

# %%

(
    fire_ts_df.select(
        F.sum("NumAlarms"),
        F.avg("ResponseDelayedinMins"),
        F.min("ResponseDelayedinMins"),
        F.max("ResponseDelayedinMins"),
    ).show()
)

# %%
