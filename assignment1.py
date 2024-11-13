from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, udf
from pyspark.sql.types import DoubleType



spark = SparkSession.builder.getOrCreate()
df = spark.read.csv("hw_200.csv", inferSchema=True, header=True)
df.printSchema()

df_ = df.withColumn(
    "interpretedHeight", when(trim(col("Height(Inches)")).rlike("\d+"), trim(col("Height(Inches)")).cast("double")).otherwise(None)
).withColumn(
    "interpretedWeight", when(trim(col("Weight(Pounds)")).rlike("\d+"), trim(col("Weight(Pounds)")).cast("double")).otherwise(None)
)

avg_height, avg_weight = df_.select(["interpretedHeight", "interpretedWeight"]).groupBy().avg().collect()[0]
print(f'''avg height : {avg_height}
avg_weight : {avg_weight}''')

print("Records where weight is greater than average weight.")
df_[df_["interpretedWeight"] > avg_weight].show()

bmiCalculate = udf(lambda weight, height: ((weight / 2.205) / (height * 0.0254) ** 2) if (weight and height) else None, DoubleType())
df_with_bmi = df_.withColumn("BMI", bmiCalculate(df_["interpretedWeight"], df_["interpretedHeight"]))
print("Dataframe with added column BMI.")
df_with_bmi.show()

df_with_bmi.write.parquet("hw_200_parquet")