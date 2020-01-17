textDataRDD = sc.textFile("file:///home/karem1a/preprod3/PGA_Tour_2010_2018_preprod.csv");
	
type(textDataRDD)
textDataRDD.take(50)
#header = textDataRDD.first()
#type(header)
#header
#textDataRDD = textDataRDD.filter(lambda x:x != header)
#textDataRDD.take(50)
textDataDF = textDataRDD.map(lambda x: x.split(",")).toDF()
type(textDataDF)
textDataDF.show(50)
from pyspark.sql import Row

textDataDF = textDataDF.rdd.map(lambda x: Row(player_name = x[0], statistic = x[1], variable = x[2], value = x[3], season = x[4])).toDF()
textDataDF.show(50)
textDataDF.printSchema()
textDataDF.show(50)
#textDataDF.createOrReplaceTempView("PGATours20102018") 
#spark.sql("select count(*) from PGATours20102018").show()
#spark.sql("select * from pgatours20102018").show()
spark = SparkSession.builder\
        .getOrCreate()
#spark.sql("select count(*) from PGATours20102018_Final").show()
#spark.sql("create external table default.PGATours20102018_p1(player_name string, statistics string, variable string, value string)  PARTITIONED BY (season int) LOCATION '/user/karem1a/Hw2-data-sample/' as select * from PGATours20102018");
#spark.sql("select count(*) from PGATours20102018_Final").show()
spark.sql("show databases").show()
spark.sql("drop table IF EXISTS default.PGATours20102018").show()

# Location : hdfs://ChipCloud1/user/karem1a/HW2-data
spark.sql("set hive.exec.dynamic.partition = true")
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
spark.sql("set hive.exec.max.dynamic.partitions=100")
spark.sql("set hive.exec.max.dynamic.partitions.pernode=100")


spark.sql("Create external table IF NOT EXISTS default.PGATours20102018(player_name string, statistics string, variable string, value string, season int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'  LOCATION '/user/karem1a/preprod3/' ").show()
spark.sql("select count(*) from pgatours20102018").show()
#spark.sql("select * from pgatours20102018").show()
spark.sql("drop table IF EXISTS default.PGATours20102018_p1").show()
spark.sql("Create external table IF NOT EXISTS default.PGATours20102018_p1(player_name string, statistic string, variable string, value string) PARTITIONED BY (season int)   LOCATION '/user/karem1a/preprod3/'").show()
#spark.sql("Create external table IF NOT EXISTS default.PGATours20102018_p1(player_name string, statistic string, variable string, value string) PARTITIONED BY (season int) stored in sequencefile").show() 
spark.sql("describe Formatted PGATours20102018_p1").show()
spark.sql("select count(*) from pgatours20102018_p1").show()
#spark.sql("select * from pgatours20102018_p1").show()
spark.sql("Insert into table default.PGATours20102018_p1 partition (season) select player_name, statistics, variable, value, season from default.PGATours20102018").show()
spark.sql("select count(*) from pgatours20102018_p1").show()
spark.sql("show partitions PGATours20102018_p1").show()



spark.sql("SELECT a.player_name FROM pgatours20102018_p1 a WHERE a.season = 2012 and a.statistic = 'FedExCup Season Points' and a.value in (select max(value) from pgatours20102018_p1 b where b.season = 2012 and b.statistic= 'FedExCup Season Points')").show()
#spark.time()
exit()

#from pyspark.sql import HiveContext
#hive_context = HiveContext(sc)

#PGATour20102018Data = hive_context.table("default.PGATour20102018Data")
#PGATour20102018Data.show()
#textDataDF.write.format("orc").saveAsTable("employees")


# Store data frame into hive table
#textDataDF.write.format("ORC").saveAsTable("default.PGATour20102018Data")


