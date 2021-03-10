# %%
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("ch3_questions").getOrCreate()

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

fire_df = (
    fire_df.withColumn("IncidentDate", F.to_timestamp(F.col("CallDate"), "MM/dd/yyyy"))
    .drop("CallDate")
    .withColumn("OnWatchDate", F.to_timestamp(F.col("WatchDate"), "MM/dd/yyyy"))
    .drop("WatchDate")
    .withColumn(
        "AvailableDtTS", F.to_timestamp(F.col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a")
    )
    .drop("AvailableDtTm")
)

fire_df.show()

fire_df.count()

# %% What were all the different types of fire calls in 2018?

fire_df.select("CallType").where(F.year("IncidentDate") == 2018).distinct().count()

# %% What months within the year 2018 saw the highest number of fire calls?


(
    fire_df.where(F.year("IncidentDate") == 2018)
    .groupBy(F.month("IncidentDate").alias("month"))
    .count()
    .orderBy("count", ascending=False)
    .limit(1)
    .show()
)

# %% What months within the year 2018 saw the highest number of fire calls?

from pyspark.sql import Window

w = Window.partitionBy()
(
    fire_df.where(F.year("IncidentDate") == 2018)
    .groupBy(F.month("IncidentDate").alias("month"))
    .count()
    .withColumn("max_col", F.max("count").over(w))
    .where(F.col("Count") == F.col("max_col"))
    .drop("max_col")
    .show()
)

# %% Which neighborhood in San Francisco generated the most fire calls in 2018?

# %% Which neighborhoods had the worst response times to fire calls in 2018?

# %% Which week in the year in 2018 had the most fire calls?

# %% Is there a correlation between neighborhood, zip code, and number of fire calls?

# %% How can we use Parquet files or SQL tables to store this data and read it back?
