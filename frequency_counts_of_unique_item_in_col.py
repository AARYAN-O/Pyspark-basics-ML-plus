# difference between using rows and tuples is that when you use rows you can directly put the names of the columns where as when you use lists , you need to directly use spark.create.createDataFrame(data,["col1_name","col2_namee"])
from pyspark.sql import Row
data = [
Row(name='John', job='Engineer'),
Row(name='John', job='Engineer'),
Row(name='Mary', job='Scientist'),
Row(name='Bob', job='Engineer'),
Row(name='Bob', job='Engineer'),
Row(name='Bob', job='Scientist'),
Row(name='Sam', job='Doctor'),
]
# create DataFrame
df = spark.createDataFrame(data)

# show DataFrame
df.show()

df.groupBy("job").count().show()
