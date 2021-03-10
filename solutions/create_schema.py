# %%
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Example-3_6").getOrCreate()

# %%

# Define schema for our data using DDL
schema = "`Id` INT, `First` STRING, `Last` STRING, `Url` STRING, `Published` STRING, `Hits` INT, `Campaigns` ARRAY<STRING>"

# Create our static data
data = [
    [
        1,
        "Jules",
        "Damji",
        "https://tinyurl.1",
        "1/4/2016",
        4535,
        ["twitter", "LinkedIn"],
    ],
    [
        2,
        "Brooke",
        "Wenig",
        "https://tinyurl.2",
        "5/5/2018",
        8908,
        ["twitter", "LinkedIn"],
    ],
    [
        3,
        "Denny",
        "Lee",
        "https://tinyurl.3",
        "6/7/2019",
        7659,
        ["web", "twitter", "FB", "LinkedIn"],
    ],
    [4, "Tathagata", "Das", "https://tinyurl.4", "5/12/2018", 10568, ["twitter", "FB"]],
    [
        5,
        "Matei",
        "Zaharia",
        "https://tinyurl.5",
        "5/14/2014",
        40578,
        ["web", "twitter", "FB", "LinkedIn"],
    ],
    [
        6,
        "Reynold",
        "Xin",
        "https://tinyurl.6",
        "3/2/2015",
        25568,
        ["twitter", "LinkedIn"],
    ],
]

# %%

# Create a DataFrame using the schema defined above
blogs_df = spark.createDataFrame(data, schema)

# Show the DataFrame; it should reflect our table above
blogs_df.show()

# Print the schema used by Spark to process the DataFrame
print(blogs_df.printSchema())

# %% Expressions

import pyspark.sql.functions as _

blogs_df.select("Hits").show(2)
blogs_df.select(_.expr("Hits * 2")).show(2)
blogs_df.select(_.col("Hits") * 2).show(2)

# %%

blogs_df.withColumn("Big Hitters", (_.expr("Hits > 10000"))).select(
    ["Id", "Big Hitters"]
).show()

# %%

expr = _.concat(_.expr("First"), _.expr("Last"), _.expr("Id"))

blogs_df.withColumn("AuthorsId", expr).select("AuthorsId").show(4)

# %%

blogs_df.select("Hits").show(2)
blogs_df.select(_.expr("Hits")).show(2)
blogs_df.select(_.col("Hits")).show(2)

# %%

blogs_df.sort("ID", ascending=False).show()
blogs_df.sort(_.col("Id"), ascending=False).show()

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
# access using index for individual items blog_row[1]

# %%

rows = [Row("Matei Zaharia", "CA"), Row("Reynold Xin", "CA")]
authors_df = spark.createDataFrame(rows, ["Authors", "State"])
authors_df.show()
