#!/bin/bash

# Sample script to automate entire process

hadoop fs -copyToLocal /data/BV/tweetsbydate-RAW.txt ./

cat tweetsbydate-RAW.txt | sed -e 's/,/-/' | sed -e 's/\//-/g' | awk -F'-' '{printf("%d-%.2d-%.2d,%d\n", $3, $1, $2, $4)}' > $$_tweetsbydate.txt

echo "CREATE TABLE tweetbydate(tweet_date DATE PRIMARY KEY, count INT);" | sqlite3 $$_bv.db

cat $$_tweetsbydate.txt | awk -F',' '{printf("INSERT INTO tweetbydate VALUES(\"%s\",%d);\n", $1, $2)}' > $$_insert-tweetsbydate.sql

sqlite3 $$_bv.db < $$_insert-tweetsbydate.sql

echo "SELECT * FROM tweetbydate;" | sqlite3 -column -header $$_bv.db

/bin/rm $$_bv.db $$_tweetsbydate.txt $$_insert-tweetsbydate.sql
