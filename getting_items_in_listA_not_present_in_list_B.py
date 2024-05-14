list_A = [1, 2, 3, 4, 5]
list_B = [4, 5, 6, 7, 8]
# when you are given lists , first step should be to convert that list into df
rdd_A=spark.sparkContext.parallelize(list_A)
rdd_B=spark.sparkContext.parallelize(list_B)
rdd_minus=rdd_A.subtract(rdd_B)
# rdd_minus.show()
# for collecting the results of rdd , we cannot directly use the show(). Rather collect the results using collect()
results=rdd_minus.collect()
print(results)
