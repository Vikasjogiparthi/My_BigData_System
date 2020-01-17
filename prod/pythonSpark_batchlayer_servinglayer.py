# initialize hive metrics
spark.sql("set hive.exec.dynamic.partition = true")
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
spark.sql("set hive.exec.max.dynamic.partitions=100")
spark.sql("set hive.exec.max.dynamic.partitions.pernode=100")

# Partition drop table script
spark.sql("drop table IF EXISTS default.PGATours20102018_p1").show()
# Partition create table script
spark.sql("Create external table IF NOT EXISTS default.PGATours20102018_p1(player_name string, statistics string, variable string, value string) PARTITIONED BY (season int)   LOCATION '/user/karem1a/Homework2/batchviews/'").show()
# describe partition table
spark.sql("describe Formatted PGATours20102018_p1").show()
spark.sql("select count(*) from pgatours20102018_p1").show()
# Insert data into partition table
spark.sql("Insert into table default.PGATours20102018_p1 partition (season) select player_name, statistics, variable, value, season from default.PGATours20102018").show()
spark.sql("select count(*) from pgatours20102018_p1").show()
# view list of partitions
spark.sql("show partitions PGATours20102018_p1").show()

#-------------------------------------------------------------------------------------
# Queries to be executed
#-------------------------------------------------------------------------------------

# Best player of the statistics 'FedExCup Season Points' and variable='2012'
spark.sql("SELECT a.player_name FROM pgatours20102018_p1 a WHERE a.season = 2012 and a.statistics = 'FedExCup Season Points' and a.value in (select max(value) from pgatours20102018_p1 b where b.season = 2012 and b.statistics= 'FedExCup Season Points')").show()

# List of players with maximum values of variable like (MONEY) in 2011
spark.sql("Select player_name, value from pgatours20102018_p1 where value in (SELECT max(value) as Highest_money_leader FROM pgatours20102018_p1 a WHERE a.season = 2011 and a.variable like '%(MONEY)%')").show()


# count of distinct players in 2014 
 spark.sql("SELECT count(distinct(b.player_name)) from pgatours20102018_p1 b where b.season=2014 and  b.statistics in (SELECT DISTINCT(a.statistics) from pgatours20102018_p1 a where a.season = 2014)").show()

 # Distinct names of statistics in 2015 like Cup 
 spark.sql("select distinct(a.statistics) from pgatours20102018_p1 a where a.season = 2015 and a.statistics like '%Cup%'").show()



exit()