#!/bin/bash
# Sample script to automate Filteration and preprocessing of the PGA Tour RAW data set

#-------------------------------------------------------------------------------------------
#Filteration and preprocessing
#-------------------------------------------------------------------------------------------

# sed 's/,//5' : Remove the 5th comma(,) from the "VALUE" attribute (Example : 1,438 is converted as 1438) 
# sed 's/"//g' : Remove the " from the entire file, as the " are considered as extra string 
cat PGA-Tour-2010-2018.txt | sed 's/,//5' | sed 's/"//g' > PGA_Tour_2010_2018_filtered.txt

# Shifting the $2 column data to $5 and moving the respective columns
cat PGA_Tour_2010_2018_filtered.txt | awk -F',' '{printf("%s,%s,%s,%s,%d\n", $1, $3,$4, $5, $2)}' > PGA_Tour_2010_2018_preprod.txt

# Remove the first column in the file
sed -i '1d'  PGA_Tour_2010_2018_preprod.txt 

# If $4 value is "" replace with 0 else remain same.
cat PGA_Tour_2010_2018_preprod.txt | awk -F',' '{ if ( $4=="" ) {printf("%s,%s,%s,%s,%d\n", $1, $2, $3, "0", $5)} else {printf("%s,%s,%s,%s,%d\n", $1, $2, $3, $4, $5)} }' > PGA_Tour_2010_2018_preprodtest.txt

#-------------------------------------------------------------------------------------------
# Data ingestion: table creation in to sqlite3
#-------------------------------------------------------------------------------------------

# Drop table from sqlite3
echo "Drop table if exists PGA_Tour_2010_2018;" | sqlite3 PGA_preProdtest.db

# Table creation in sqlite3
echo "CREATE TABLE PGA_Tour_2010_2018(PLAYER_NAME TEXT NOT NULL, STATISTIC TEXT NOT NULL, VARIABLE TEXT NOT NULL,VALUE NUMERIC NOT NULL, SEASON INT NOT NULL);" | sqlite3 PGA_preprodtest.db

# Insert script generation for the 2.5 M records
cat PGA_Tour_2010_2018_preprodtest.txt | awk -F',' '{printf("INSERT INTO PGA_Tour_2010_2018 VALUES(\"%s\",%d,\"%s\",\"%s\",\"%s\");\n", $1, $2, $3, $4, $5)}' > insert_PGA_Tour_2010_2018_filtered.sql

# Place the insert script into databases for loading 
sqlite3 PGA_test.db < insert_PGA_Tour_2010_2018_filtered.sql

# Display the records
echo "SELECT * FROM PGA_Tour_2010_2018;" | sqlite3 -column -header PGA_test.db

# Delete sql file and filtered data
/bin/rm PGA_Tour_2010_2018_filtered.txt insert_PGA_Tour_2010_2018_filtered.sql PGA_Tour_2010_2018_preprod.txt 