import requests
from json import dump, loads
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, lower, trim

response = requests.get("https://jsonplaceholder.typicode.com/users").json()
with open("apiData.json", "w+") as f:
    dump(response, f)


spark = SparkSession.builder.getOrCreate()
df = spark.read.json("apiData.json")
df.printSchema()



df_ = df.select(
    *df.columns,
    col("address.city").alias("cityName"),
    col("company.name").alias("companyName"),
    col("address.geo.lat").alias("latitude"),
).withColumn(
    "address", struct(
        col("address.street"),
        col("address.suite"),
        col("address.zipcode"),
        struct(
            col("address.geo.lng")
        ).alias("geo")
    )
).withColumn(
    "company", struct(
        col("company.bs"),
        col("company.catchPhrase"),
    )
)

df_.printSchema()

print("Records which have crona in there company name.")
df_.filter(condition=lower(trim(df_["companyName"])).contains("crona")).show()

print("Record count for each distinct city")
df_.withColumn(
    "modifiedCityName",
    lower(trim(col("cityName")))
).groupBy("modifiedCityName").count().show()



data = df_.toJSON().collect()
data = map(lambda x:loads(x), data)

with open("modifiedApiData.json", "w+") as f:
    dump(list(data), f)