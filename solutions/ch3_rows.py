# %%
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ch3_rows").getOrCreate()

# %%

from pyspark.sql import Row

blog_row = Row(
    6,
    "Reynold",
    "Xin",
    "https://tinyurl.6",
    255568,
    "3/2/2015",
    ["twitter", "LinkedIn"],
)

blog_row

# %%

rows = [Row("Matei Zaharia", "CA"), Row("Reynold Xin", "CA")]
authors_df = spark.createDataFrame(rows, ["Authors", "State"])
authors_df.show()

# %%
