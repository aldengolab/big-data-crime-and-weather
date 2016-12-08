REGISTER /usr/local/mpcs53013/pig/piggybank-0.16.0.jar;
REGISTER /usr/local/mpcs53013/mvn/elephant-bird-core-4.14.jar;
REGISTER /usr/local/mpcs53013/mvn/elephant-bird-pig-4.14.jar;
REGISTER /usr/local/mpcs53013/mvn/elephant-bird-hadoop-compat-4.14.jar;
REGISTER /usr/local/mpcs53013/mvn/libthrift-0.9.0.jar;
register /home/mpcs53013/workspace/IngestCrime/target/IngestWeather-0.0.1-SNAPSHOT.jar

WEATHER = LOAD '/inputs/chicago_weather' USING PigStorage(',') 
as (year:int, month:int, day:int,
  meanTemperature:double, meanVisibility: double, 
    meanWindSpeed:double, fog:long, rain:long, snow:long,  hail:long, thunder:long, tornado:long, clear:long);

DEFINE WSThriftBytesToTuple com.twitter.elephantbird.pig.piggybank.ThriftBytesToTuple('agolab.CrimeAndWeatherApp.crimeEvent.CrimeEvent');
RAW_CRIME = LOAD '/inputs/thriftCrime/chicagoCrime' USING org.apache.pig.piggybank.storage.SequenceFileLoader() as (key:long, value: bytearray);
CRIME_EVENTS = FOREACH RAW_CRIME GENERATE FLATTEN(WSThriftBytesToTuple(value));

CRIME_AND_WEATHER_RAW = JOIN WEATHER by (year, month, day), CRIME_EVENTS by (CrimeEvent::year, CrimeEvent::month, CrimeEvent::day) PARALLEL 5;

CRIME_AND_WEATHER = FOREACH CRIME_AND_WEATHER_RAW GENERATE
	CrimeEvent::year, CrimeEvent::month, CrimeEvent::day, CrimeEvent::hour, 
	CrimeEvent::primaryType, 
	meanTemperature, fog, rain, snow, hail, thunder, tornado, clear;

STORE CRIME_AND_WEATHER into '/inputs/chicagoCrimeWeather' USING PigStorage(',');