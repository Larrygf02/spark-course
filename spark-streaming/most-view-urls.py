from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession

from pyspark.sql.functions import regexp_extract
import pyspark.sql.functions as func

spark = SparkSession.builder.config("spark.sql.warehouse.dir", "temp").appName("StructuredStreaming").getOrCreate()
spark.sparkContext.setLogLevel("ERROR") #only show error logs
accessLines = spark.readStream.text("logs")

contentSizeExp = r'\s(\d+)$'
statusExp = r'\s(\d{3})\s'
generalExp = r'\"(\S+)\ss(\S+)\s*(\S*)\"'
timeExp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
hostExp = r'(^\S+\.[\S+\.]+\S+)\s'

logsDF = accessLines.select(regexp_extract('value', hostExp, 1).alias('host'),
                            regexp_extract('value', timeExp, 1).alias('timestamp'),
                            regexp_extract('value', generalExp, 1).alias('method'),
                            regexp_extract('value', generalExp, 2).alias('endpoint'),
                            regexp_extract('value', generalExp, 3).alias('protocol'),
                            regexp_extract('value', statusExp, 1).cast('integer').alias('status'),
                            regexp_extract('value', contentSizeExp, 1).cast('integer').alias('content_size'))

logsDF2 = logsDF.withColumn("eventTime", func.current_timestamp())

endpointCounts = logsDF2.groupBy(func.window(func.col("eventTime"), \
                "30 seconds", "10 seconds"), func.col("endpoint")).count()

sortedEndpointCounts = endpointCounts.orderBy(func.col("count").desc())

query = sortedEndpointCounts.writeStream.outputMode("complete").format("console") \
        .queryName("counts").start()

query.awaitTermination()

spark.stop()