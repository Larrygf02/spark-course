from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("FriendsByAge").getOrCreate()

lines = spark.read.option("header", "true").option("inferSchema", "true").csv("fakefriends-header.csv")

# select column age and friends
friendsByAge = lines.select("age", "friends")

# group by age and then compute average
friendsByAge.groupBy("age").avg("friends").show()

# sorted
friendsByAge.groupBy("age").avg("friends").sort("age").show()

# formatted nicely
friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2)).sort("age").show()

# with a custom name
friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2).alias("friends_avg")).sort("age").show()

spark.stop()

