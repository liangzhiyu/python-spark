from pyspark.sql import SparkSession
import pyspark.sql.functions as fn
import pyspark.sql.types as type
from pyspark import SparkConf, SparkContext

# import bokeh.charts as chrt
from bokeh.io import output_notebook

spark = SparkSession.builder.appName("dataFrameTest").getOrCreate()
# df = spark.read.text('DataFrameTest.py')
# df.show()

# sc = SparkContext.getOrCreate()
# json = sc.parallelize(['liangzy', 'man'])
# people = spark.read.json(json)
# people.show()

df = spark.createDataFrame([
    (1, 144.5, 5.9, 33, 'M'),
    (2, 167.2, 5.4, 45, 'M'),
    (3, 124.1, 5.2, 23, 'F'),
    (4, 144.5, 5.9, 33, 'M'),
    (5, 133.2, 5.7, 54, 'F'),
    (3, 124.1, 5.2, 23, 'F'),
    (5, 129.2, 5.3, 42, 'M'),
],
    ['id', 'weight', 'height', 'age', 'gender'])

# df.select("id", "weight").filter("id = 3").show()

print 'Count of rows: {0}'.format(df.count())
print 'Count of distinct rows: {0}'.format(df.distinct().count())
# df.distinct().show()

# 1.delete duplicates
df = df.dropDuplicates()
df.show()
print 'Count of ids: {0}'.format(df.count())
print 'Count of ids: {0}'.format(
    df.select([
        c for c in df.columns if c != 'id'
    ]).distinct().count()
)

# 2.subset: custom columns
df = df.dropDuplicates(subset=[
    c for c in df.columns if c != 'id'
])
df.show()

# agg: count or distinct count
df.agg(
    fn.count('id').alias('count'),
    fn.countDistinct('id').alias('distinct')
).show()

# new only id
df.withColumn('new_id', fn.monotonically_increasing_id()).show()

# bokeh
# output_notebook()
#
# hists = fraud_df.select('balance').rdd.flagMap(
#     lambda row: row
# ).histogram(20)
#
# data = {
#     'bins': hists[0][:-1],
#     'freq':hists[1]
# }
#
# b_hist = chrt.Bar(
#     data,
#     value='freq', label='bins',
#     title='Histogram of \'balance\'')
# chrt.show(b_hist)

spark.stop()
