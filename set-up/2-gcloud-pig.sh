echo "Setting up HBase"
hbase shell ./hb-set.txt
echo "HBase set-up complete. Deleting ouput locations..."
hdfs dfs -rm /agolab/inputs/chicago_weather/*
hdfs dfs -rmdir /agolab/inputs/chicago_weather
hdfs dfs -rm /agolab/inputs/chicagoCrimeTypes/*
hdfs dfs -rmdir /agolab/inputs/chicagoCrimeTypes
hdfs dfs -rm /agolab/inputs/chicagoCrimeWeather/*
hdfs dfs -rmdir /agolab/inputs/chicagoCrimeWeather
echo "Running pig..."
pig ~/agolab/pigFiles/1-weatherNameLoad.pig
pig ~/agolab/pigFiles/2-communityJoin.pig
pig ~/agolab/pigFiles/3-weatherCrimeJoin.pig
pig ~/agolab/pigFiles/4-crimeByCommunity.pig
pig ~/agolab/pigFiles/5-crimetypeinformation.pig
pig ~/agolab/pigFiles/6-crimeYOY.pig
pig ~/agolab/pigFiles/7-averageWeather.pig
echo "Script complete."
