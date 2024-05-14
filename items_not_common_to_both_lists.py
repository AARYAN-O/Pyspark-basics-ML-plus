list_A = [1, 2, 3, 4, 5]
list_B = [4, 5, 6, 7, 8]
rdd_A=spark.sparkContext.parallelize(list_A)
rdd_B=spark.sparkContext.parallelize(list_B)
rdd_A_minus_B=rdd_A.subtract(rdd_B)
rdd_B_minus_A=rdd_B.subtract(rdd_A)
rdd_union=rdd_A_minus_B.union(rdd_B_minus_A)
results=rdd_union.collect()
print(results)
