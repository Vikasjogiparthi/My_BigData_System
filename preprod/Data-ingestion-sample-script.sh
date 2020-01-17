#!/bin/bash
# Sample script to automate entire process
cat PGA-Tour-2010-2018.txt | sed 's/,//5' | sed 's/"//g' > PGA_Tour_2010_2018_filtered.txt
cat PGA_Tour_2010_2018_filtered.txt | awk -F',' '{printf("%s,%s,%s,%s,%d\n", $1, $3,$4, $5, $2)}' > PGA_Tour_2010_2018_preprod.txt
sed -i '1d'  PGA_Tour_2010_2018_preprod.txt

echo "Drop table if exists PGA_Tour_2010_2018;" | sqlite3 PGA_test.db
echo "CREATE TABLE PGA_Tour_2010_2018(PLAYER_NAME TEXT NOT NULL, SEASON INT NOT NULL, STATISTIC TEXT NOT NULL, VARIABLE TEXT NOT NULL, VALUE NUMERIC NOT NULL);" | sqlite3 PGA_test.db
cat PGA_Tour_2010_2018_filtered.txt | awk -F',' '{printf("INSERT INTO PGA_Tour_2010_2018 VALUES(\"%s\",%d,\"%s\",\"%s\",\"%s\");\n", $1, $2, $3, $4, $5)}' > insert_PGA_Tour_2010_2018_filtered.sql
sqlite3 PGA_test.db < insert_PGA_Tour_2010_2018_filtered.sql
echo "SELECT * FROM PGA_Tour_2010_2018;" | sqlite3 -column -header PGA_test.db
/bin/rm PGA_Tour_2010_2018_filtered.txt insert_PGA_Tour_2010_2018_filtered.sql