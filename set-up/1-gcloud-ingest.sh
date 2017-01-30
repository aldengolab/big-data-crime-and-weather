yarn jar ~/agolab/jars/uber-IngestCrime-0.0.1-SNAPSHOT.jar agolab.CrimeWeatherApp.IngestCrime.SerializeCrimeEvent /home/mpcs53013/agolab/crimeData/crime.csv
yarn jar ~/agolab/jars/uber-IngestWeather-0.0.1-SNAPSHOT.jar agolab.CrimeWeatherApp.IngestWeather.SerializeWeatherSummary  /home/mpcs53013/agolab/weatherData/
hdfs dfs -mkdir /agolab/inputs/weathergeo
hdfs dfs -mkdir /agolab/inputs/crimeGeo
hdfs dfs -put ~/agolab/weatherData/station-codes.txt /agolab/inputs/weathergeo
hdfs dfs -put ~/agolab/crimeData/comms_names.csv /agolab/inputs/crimeGeo
