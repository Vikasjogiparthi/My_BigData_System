textDataRDD = sc.textFile("file:///home/karem1a/preprodfinal/PGA_Tour_2010_2018_preprodtest.csv");
	
type(textDataRDD)
textDataRDD.take(50)

textDataDF = textDataRDD.map(lambda x: x.split(",")).toDF()
type(textDataDF)
textDataDF.show(50)
from pyspark.sql import Row

textDataDF = textDataDF.rdd.map(lambda x: Row(player_name = x[0], statistic = x[1], variable = x[2], value = x[3], season = x[4])).toDF()
textDataDF.show(50)
textDataDF.printSchema()
textDataDF.show(50)

spark = SparkSession.builder\
        .getOrCreate()

spark.sql("show databases").show()
spark.sql("drop table IF EXISTS default.PGATours20102018").show()

# Location : hdfs://ChipCloud1/user/karem1a/HW2-data
spark.sql("set hive.exec.dynamic.partition = true")
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
spark.sql("set hive.exec.max.dynamic.partitions=100")
spark.sql("set hive.exec.max.dynamic.partitions.pernode=100")


spark.sql("Create external table IF NOT EXISTS default.PGATours20102018(player_name string, statistics string, variable string, value string, season int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'  LOCATION '/user/karem1a/preprodfinal/' ").show()
spark.sql("select count(*) from pgatours20102018").show()

exit()


