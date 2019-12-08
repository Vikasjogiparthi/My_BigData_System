textDataRDD = sc.textFile("file:///home/dir_name/HW2-Data/PGA-Tour-2010-2018-filtered-2ndstage.csv");
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
 
# Store data frame into hive table
textDataDF.write.format("ORC").saveAsTable("db_bdp.PGA-Tour-2010-2018-Data")