# Zip is an in-built function in Python used to iterate over multiple iterables. It takes corresponding elements from all the iterable passed to it and merges them in a tuple. 
#  PySpark parallelize() is a function in SparkContext and is used to create an RDD from a list collection.
list1 = ["a", "b", "c", "d"]
list2 = [1, 2, 3, 4]
rdd=spark.sparkContext.parallelize(list(zip(list1,list2)))
df=rdd.toDF(["Col1","Col2"])
df.show()
