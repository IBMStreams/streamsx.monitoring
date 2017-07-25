#!/bin/sh
# CREATE APPLICATION CONFIG
#echo domainID: $1
#echo instanceID: $2
#echo user: $3
#echo password: $4
#echo jmxConnect: $5
#echo sslOption: $6
#echo appConfig: $7

# add further parameters like --zkconnect if required for the streamtool commands below

echo $4 | streamtool rmappconfig --noprompt -d $1 -i $2 -U $3 $7
echo $4 | streamtool mkappconfig --property connectionURL=$5 --property user=$3 --property password=$4 --property sslOption=$6 -d $1 -i $2 -U $3 $7



