import pyspark.ml.feature as ft
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("vectorAssemblerTest").getOrCreate()
df = spark.createDataFrame(
    [(12, 10, 3), (1, 4, 2)],
    ['a', 'b', 'c'])
df2 = ft.VectorAssembler(inputCols=['a', 'b', 'c'],
                         outputCol='features') \
    .transform(df)
    # .select('features') \
    # .collect()
spark.stop()
