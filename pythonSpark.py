textDataRDD = sc.textFile("file:///home/dir_name/HW2-data/PGA-Tour-2010-2018-filtered-2ndstage.csv");
	
type(textDataRDD)
textDataRDD.take(5)
header = textDataRDD.first()
type(header)
header
textDataRDD = textDataRDD.filter(lambda x:x != header)
textDataRDD.take(5)
textDataDF = textDataRDD.map(lambda x: x.split(",")).toDF()
type(textDataDF)
textDataDF.show(5)
from pyspark.sql import Row

textDataDF = textDataDF.rdd.map(lambda x: Row(player_name = x[0], season = x[1], statistic = x[2], variable = x[3], value = x[4])).toDF()
textDataDF.printSchema()
spark = SparkSession.builder\
        .getOrCreate()
spark.sql("show databases").show()
spark.sql("drop table IF EXISTS default.PGATours20102018").show()

# Location : hdfs://ChipCloud1/user/dir_name/HW2-data
spark.sql("Create external table IF NOT EXISTS default.PGATours20102018(player_name string, season int, statistics string, variable string, value string)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' LOCATION '/user/dir_name/HW2-data/'").show()

spark.sql("describe Formatted PGATours20102018").show()
spark.sql("select count(*) from pgatours20102018").show()
spark.sql("set hive.exec.dynamic.partition = true")
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
spark.sql("set hive.exec.max.dynamic.partitions=200000")
spark.sql("set hive.exec.max.dynamic.partitions.pernode=200000")
spark.sql("drop table IF EXISTS default.PGATours20102018_p1").show()
spark.sql("Create external table IF NOT EXISTS default.PGATours20102018_p1(player_name string, season int, variable string, value string) PARTITIONED BY (statistics string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' LOCATION '/user/dir_name/HW2-data/'").show()

#spark.sql("Insert into table default.PGATours20102018_p1 partition (statistics) select player_name, season, statistics, variable, value from default.PGATours20102018").show()
#spark.sql("show partitions PGATours20102018_p1").show()

spark.sql("select count(*) from pgatours20102018_p1").show()

spark.sql("SELECT a.player_name FROM pgatours20102018_p1 a WHERE a.season = 2012 and a.statistics = 'Fairway Bunker Tendency' and a.value in (select max(value) from pgatours20102018 b where b.season = 2012 and b.statistics= 'Fairway Bunker Tendency')").show()
#spark.time()
exit()

#from pyspark.sql import HiveContext
#hive_context = HiveContext(sc)

#PGATour20102018Data = hive_context.table("default.PGATour20102018Data")
#PGATour20102018Data.show()
#textDataDF.write.format("orc").saveAsTable("employees")


# Store data frame into hive table
#textDataDF.write.format("ORC").saveAsTable("default.PGATour20102018Data")


