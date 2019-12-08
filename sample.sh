#!/bin/sh
while read details
do
PLAYER_NAME=`echo $details | awk -F "," '{print $1}'`
SEASON=`echo $details | awk -F "," '{print $2}'`
STATISTIC=`echo $details | awk -F "," '{print $3}'`
VARIABLE=`echo $details | awk -F "," '{print $4}'`
VALUE=`echo $details | awk -F "," '{print $5}'`
echo "insert into PGA_Tour_2010_2018 (${PLAYER_NAME},\"${SEASON}\",\"${STATISTIC}\",\"${VARIABLE}\",\"${VALUE}\");"

done < example