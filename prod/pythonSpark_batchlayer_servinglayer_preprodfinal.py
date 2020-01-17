spark.sql("set hive.exec.dynamic.partition = true")
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
spark.sql("set hive.exec.max.dynamic.partitions=100")
spark.sql("set hive.exec.max.dynamic.partitions.pernode=100")

#spark.sql("select * from pgatours20102018").show()
spark.sql("drop table IF EXISTS default.PGATours20102018_p1").show()
spark.sql("Create external table IF NOT EXISTS default.PGATours20102018_p1(player_name string, statistic string, variable string, value string) PARTITIONED BY (season int)   LOCATION '/user/karem1a/Homework2/batchviews/'").show()
#spark.sql("Create external table IF NOT EXISTS default.PGATours20102018_p1(player_name string, statistic string, variable string, value string) PARTITIONED BY (season int) stored in sequencefile").show() 
spark.sql("describe Formatted PGATours20102018_p1").show()
spark.sql("select count(*) from pgatours20102018_p1").show()
#spark.sql("select * from pgatours20102018_p1").show()
spark.sql("Insert into table default.PGATours20102018_p1 partition (season) select player_name, statistics, variable, value, season from default.PGATours20102018").show()
spark.sql("select count(*) from pgatours20102018_p1").show()
spark.sql("show partitions PGATours20102018_p1").show()


# Best player of the statistic 'FedExCup Season Points' and variable='2012'
spark.sql("SELECT a.player_name FROM pgatours20102018_p1 a WHERE a.season = 2012 and a.statistic = 'FedExCup Season Points' and a.value in (select max(value) from pgatours20102018_p1 b where b.season = 2012 and b.statistic= 'FedExCup Season Points')").show()

# List of players with maximum values of variable like (MONEY)
spark.sql("Select player_name, value from pgatours20102018_p1 where value in (SELECT max(value) as Highest_money_leader FROM pgatours20102018_p1 a WHERE a.season = 2011 and a.variable like '%(MONEY)%')").show()


# count of distinct players in 2014 
 spark.sql("SELECT count(distinct(b.player_name)) from pgatours20102018_p1 b where b.season=2014 and  b.statistics in (SELECT DISTINCT(a.statistics) from pgatours20102018_p1 a where a.season = 2014)").show()

 # 


exit()