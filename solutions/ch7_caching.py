# %%

import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("ch7-caching").getOrCreate()

# %%


df = (
    spark.range(1 * 10000000).toDF("id").withColumn("square", F.col("id") * F.col("id"))
)

df.cache()  # Cache the data

df.count()  # Materialize the cache

# %%

# Fast count
df.count()

# %%


df2 = (
    spark.range(1 * 20000000).toDF("id").withColumn("square", F.col("id") * F.col("id"))
)

df2.persist(pyspark.StorageLevel.DISK_ONLY)
df2.count()  # Materialize the cache

# %%

# Fast count
df2.count()

# %%
