REGISTER /usr/local/mpcs53013/pig/piggybank-0.16.0.jar;
REGISTER /usr/local/mpcs53013/mvn/elephant-bird-core-4.14.jar;
REGISTER /usr/local/mpcs53013/mvn/elephant-bird-pig-4.14.jar;
REGISTER /usr/local/mpcs53013/mvn/elephant-bird-hadoop-compat-4.14.jar;
REGISTER /usr/local/mpcs53013/mvn/libthrift-0.9.0.jar;
register /home/mpcs53013/workspace/IngestWeather/target/IngestWeather-0.0.1-SNAPSHOT.jar;

DEFINE WSThriftBytesToTuple com.twitter.elephantbird.pig.piggybank.ThriftBytesToTuple('edu.uchicago.mpcs53013.weatherSummary.WeatherSummary');
STATION_CODE_LINES = LOAD '/inputs/weathergeo/station_codes.txt' as line;
STATIONS_FILTERED = FILTER STATION_CODE_LINES by SUBSTRING($0, 10, 11) eq ' ' and SUBSTRING($0, 7, 8) neq ' ';
STATION_CODES = FOREACH STATIONS_FILTERED GENERATE (INT) SUBSTRING($0, 0, 6) as code, SUBSTRING($0, 7, 10) as name, SUBSTRING($0, 14, 33) as longName;

RAW_WEATHER_DATA = LOAD '/inputs/thriftWeather/weather-*' USING org.apache.pig.piggybank.storage.SequenceFileLoader() as (key:long, value: bytearray);
WEATHER_SUMMARY = FOREACH RAW_WEATHER_DATA GENERATE FLATTEN(WSThriftBytesToTuple(value));

WEATHER_WITH_STATION_NAME_RAW = JOIN WEATHER_SUMMARY by station, STATION_CODES by code PARALLEL 5;
WEATHER_WITH_STATION = FOREACH WEATHER_WITH_STATION_NAME_RAW GENERATE 
station, name, longName, year, month, day,
  meanTemperature, meanVisibility, meanWindSpeed, fog, rain, snow,
  hail, thunder, tornado,
  (fog == 0 AND rain == 0 AND snow == 0 AND hail == 0 AND thunder == 0 AND tornado == 0 ? 1L : 0L) as clear;
CHICAGO_WEATHER_RAW = FILTER WEATHER_WITH_STATION by longName MATCHES '.*CHICAGO.*';
CHICAGO_WEATHER_GRP = GROUP CHICAGO_WEATHER_RAW BY (year, month, day);
CHICAGO_WEATHER = FOREACH CHICAGO_WEATHER_GRP GENERATE
	group.year as year,
	group.month as month, 
	group.day as day, 
	AVG($1.meanTemperature) as meanTemperature,
	AVG($1.meanVisibility) as meanVisibility,
	AVG($1.meanWindSpeed) as meanWindSpeed,
	MAX($1.fog) as fog,
	MAX($1.rain) as rain,
	MAX($1.snow) as snow,
	MAX($1.hail) as hail,
	MAX($1.thunder) as thunder,
	MAX($1.tornado) as tornado,
	MAX($1.clear) as clear;

STORE CHICAGO_WEATHER INTO '/inputs/chicago_weather' USING PigStorage(',');