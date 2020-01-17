# Sparkcontext to read the filtered file using RDD (Set Path of Filtered Data)
# Load Data into RDD
textDataRDD = sc.textFile("file:///home/karem1a/HomeWork2/batchviews/PGA_Tour_2010_2018_preprodtest.csv");
	

type(textDataRDD)
textDataRDD.take(50)
# Convert RDD to Data Frame	
textDataDF = textDataRDD.map(lambda x: x.split(",")).toDF()
type(textDataDF)
textDataDF.show(50)
from pyspark.sql import Row
# Assign the values of the each attribute to the column name
textDataDF = textDataDF.rdd.map(lambda x: Row(player_name = x[0], statistics = x[1], variable = x[2], value = x[3], season = x[4])).toDF()
textDataDF.show(50)
# Print the schema of Data frame
textDataDF.printSchema()
textDataDF.show(50)

# Initialize spark session
spark = SparkSession.builder\
        .getOrCreate()
# show databases
spark.sql("show databases").show()
spark.sql("drop table IF EXISTS default.PGATours20102018").show()

# Initialize hive metrics
spark.sql("set hive.exec.dynamic.partition = true")
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
spark.sql("set hive.exec.max.dynamic.partitions=100")
spark.sql("set hive.exec.max.dynamic.partitions.pernode=100")

# Create table for source data PGATour20102018
spark.sql("Create external table IF NOT EXISTS default.PGATours20102018(player_name string, statistics string, variable string, value string, season int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'  LOCATION '/user/karem1a/Homework2/batchviews/' ").show()
spark.sql("select count(*) from pgatours20102018").show()

exit()


