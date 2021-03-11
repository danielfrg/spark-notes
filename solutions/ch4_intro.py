# %%

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQLExampleApp").getOrCreate()

# %%

csv_file = "../LearningSparkV2/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"

df = (
    spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(csv_file)
)

df.createOrReplaceTempView("us_delay_flights_tbl")

df.show()

# %% all flights whose distance is greater than 1,000 miles:

spark.sql(
    """SELECT origin, destination, distance
    FROM us_delay_flights_tbl
    WHERE distance > 1000
    ORDER BY distance DESC"""
).show(10)

# %% all flights between San Francisco (SFO) and Chicago (ORD) with at least a two-hour delay

spark.sql(
    """SELECT *
    FROM us_delay_flights_tbl
    WHERE origin == 'SFO' AND destination == 'ORD' and delay > 120
    ORDER by delay DESC
"""
).show(10)

# %%

spark.sql(
    """ SELECT delay, origin, destination,
        CASE
            WHEN delay > 360 THEN 'Very Long Delays'
            WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
            WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
            WHEN delay > 0 and delay < 60  THEN  'Tolerable Delays'
            WHEN delay = 0 THEN 'No Delays'
            ELSE 'Early'
        END AS Flight_Delays
        FROM us_delay_flights_tbl
        ORDER BY origin, delay DESC
"""
).show(10)

# %% Only works with Hive

spark.sql(
    """
    CREATE TABLE managed_us_delay_flights_tbl (date STRING, delay INT,
      distance INT, origin STRING, destination STRING)"""
)

# %%

spark.catalog.listDatabases()

spark.catalog.listTables()

# %%
