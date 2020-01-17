# My_BigData_System
This is the project about Big Data system where we use PGA Tour 2010-2018 Data from kaggle.

Creating a Hadoop folder should be done before the process with name /Homework2. You can replace karem1a with your project name or something you can understand.

DATA INGESTION LAYER
Manual steps performed to place the ﬁles in the respective folder of DataIngestion process: 
(All the ﬁles are in the respective folders, Please run the execution steps to perform data ingestion and hive table loading) 
• "/home/karem1a/Homework2/Data_preprocessing_sqlite3_insertion.sh"(Filtrationandsqlite ingestion)
– hadoop fs -put Data_preprocessing_sqlite3_insertion.sh /user/karem1a/Homework2/
[Note: sqlite insertion operation takes longer duration approximately 90 min ] 
Execution step: hadoop fs -cat /user/karem1a/Homework2/Data_preprocessing_sqlite3_insertion.sh | exec sh 
• "/home/karem1a/Homework2/pythonSpark_dataingestion_hiveloading.py" 
 – Cross validate PGA_Tour_2010_2018_preprodtest.csv exists in /user/karem1a/Homework2 /batchviews/ of HDFS
 – If the above step is validated, Execute the python script, if not perform below operation. 
 – hadoop fs -copyFromLocal /home/karem1a/Homework2/batchviews/PGA_Tour_2010_2018_ preprodtest.csv /user/karem1a/Homework2/batchviews/
 – cd /home/karem1a/Homework2/
Executionstep: PYTHONSTARTUP=pythonSpark_dataingestion_hiveloading.py pyspark

Automated Script “pythonSpark_dataingestion_hiveloading.py” perform the above process.
BATCH LAYER:
Manual steps performed to place "pythonSpark_batchlayer_servinglayer_preprodﬁnal.py" ﬁle in therespectivefolderofBatchlayerprocess: (Alltheﬁlesareintherespectivefolders,PleaseruntheexecutionstepstoperformBatchlayerandserving layer) • "pythonSpark_batchlayer_servinglayer_preprodﬁnal.py"(Batchlayerandservinglayerprocess)
 – hadoopfs-copyFromLocal/home/karem1a/Homework2/pythonSpark_dataingestion_hiveload ing.py /user/karem1a/Homework2/ 
 – hadoop fs -copyFromLocal /home/karem1a/Homework2/pythonSpark_batchlayer_servinglay er.py /user/karem1a/Homework2/ 
 – cd /home/karem1a/Homework2/
Executionstep: PYTHONSTARTUP=pythonSpark_batchlayer_servinglayer_preprodﬁnal.pypyspark
After the execution of the Batch layer, 9 different batch views which are created based on "Season". Each season data is considered as batch views. Batch views are generated in the below-mentioned path /user/karem1a/Homework2/batchviews/

Automated Script “pythonSpark_batchlayer_servinglayer_preprodfinal.py” does this all.

Serving Layer:
In this system, we have different queries to check the latency before and after generation batch views. The queries considered are 
• 1. Best player of the statistic ’FedExCup Season Points’ and variable=’2012’ 
• 2. List of players with maximum values of variable like (MONEY) in 2011 
• 3. Count of distinct players in 2014 
• 4. Distinct names of statistics in 2015 like Cup
