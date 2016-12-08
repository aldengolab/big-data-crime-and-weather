CRIMES = LOAD '/inputs/chicagoCrimeWeather' USING PigStorage(',') 
	as (year:long, month:long, day:long, hour:long, primaryType, 
	meanTemperature:double, fog:long, rain:long, snow:long, hail:long, 
	thunder:long, tornado:long, clear:long);

GRP_CRIME = GROUP CRIMES BY primaryType;
CRIME_WEATHER = FOREACH GRP_CRIME GENERATE
	group as crime,
	COUNT_STAR($1) as count,
	AVG($1.meanTemperature) as meanTemp,
	SUM($1.fog) as fog,
	SUM($1.rain) as rain,
	SUM($1.snow) as snow,
	SUM($1.hail) as hail,
	SUM($1.thunder) as thunder,
	SUM($1.tornado) as tornado,
	SUM($1.clear) as clear;

STORE CRIME_WEATHER INTO 'hbase://crime_weather_sums'
  USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(
    'weather:count, weather:meanTemp, weather:fog, weather:rain, 
     weather:now, weather:hail, weather:thunder, weather:tornado, 
     weather:clear');