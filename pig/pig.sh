hdfs dfs -rm /inputs/chicago*/*
hdfs dfs -rmdir /inputs/chicago*
pig 1-weatherNameLoad.pig
pig 2-communityJoin.pig
pig 3-weatherCrimeJoin.pig
pig 4-crimeByCommunity.pig
pig 5-crimetypeinformation.pig
pig 6-crimeYOY.pig
pig 7-averageWeather.pig