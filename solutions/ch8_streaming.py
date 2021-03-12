# %%

import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("ch7-streaming").getOrCreate()

# %% Step 1: Define input sources

lines = (
    spark.readStream.format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()
)

# %% Step 2: Transform data

words = lines.select(F.split(F.col("value"), "\\s").alias("word"))

counts = words.groupBy("word").count().orderBy("count", ascending=False)

# %% Step 3: Define output sink and output mode

writer = counts.writeStream.format("console").outputMode("complete")

# %% Step 4: Specify processing details

# checkpointDir = "./checkpoints"
# writer2 = writer.trigger(processingTime="1 second").option(
#     "checkpointLocation", checkpointDir
# )

writer2 = writer.trigger(processingTime="1 second")

# %% Be sure there is somethign running on port 9999 or ir will fail

"""
We can send data to this port using:
    nc -l -p 9999
"""

# %% Step 5: Start the query

streamingQuery = writer2.start()

# Use this when running as script to see the outpu
streamingQuery.awaitTermination()

streamingQuery.status

# %% Check status

streamingQuery.status


# %% Monitor

streamingQuery.lastProgress

# %%

streamingQuery.awaitTermination()

# %%
