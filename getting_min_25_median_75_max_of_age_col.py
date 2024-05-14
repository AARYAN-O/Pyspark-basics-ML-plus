data = [("A", 10), ("B", 20), ("C", 30), ("D", 40), ("E", 50), ("F", 15), ("G", 28), ("H", 54), ("I", 41), ("J", 86)]
# rdd-spark.sparkContext.paralleize(data,["Name","Age"])  --> this will create rdd and not dataframe
# there are two ways to create dataframes --> 1) use createDataFrame directly 2) convert rdd to df using toDF()
df=spark.createDataFrame(data,["Name","Age"])
df.show()

######################

quantiles=df.approxQuantile("Age",[0.0,0.25,0.50,0.75,1.0],0.01)
# putting relative errors are necessary
for i in range(len(quantiles)):
    print(quantiles[i])
