from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
from pyspark.ml.recommendation import ALS
import sys
import codecs

def loadMovieName():
    movieNames = {}
    with codecs.open("ml-100k/u.ITEM", "r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

spark = SparkSession.builder.appName("ALSExample").getOrCreate()

movieSchema = StructType([ \
                StructField("userID", IntegerType(), True), \
                StructField("movieID", IntegerType(), True), \
                StructField("rating", IntegerType(), True), \
                StructField("timestamp", LongType(), True), ])

names = loadMovieName()

ratings = spark.read.option("sep", "\t").schema(movieSchema) \
        .csv("ml-100k/u.data")

print("Training recommendation model ...")

als = ALS().setMaxIter(5).setRegParam(0.01).setUserCol("userID").setItemCol("movieID") \
        .setRatingCol("rating")

model = als.fit(ratings)

## manually construct a dataframe of the user ID's we want recs for
userID = int(sys.argv[1])
userSchema = StructType([StructField("userID", IntegerType(), True)])
users = spark.createDataFrame([[userID,]], userSchema)

recommendations = model.recommendForUserSubset(users, 10).collect()

print("Top 10 recommendations for user id: " + str(userID))

for userRecs in recommendations:
    myRecs = userRecs[1]
    for rec in myRecs:
        movie = rec[0]
        rating = rec[1]
        movieName = names[movie]
        print(movieName + " " +str(rating))