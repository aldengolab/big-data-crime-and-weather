WEATHER = LOAD '/inputs/chicago_weather' USING PigStorage(',') 
as (year:int, month:int, day:int,
  meanTemperature:double, meanVisibility: double, 
    meanWindSpeed:double, fog:long, rain:long, snow:long,  hail:long, thunder:long, tornado:long, clear:long);

AVG_WEATHER_GRP = GROUP WEATHER ALL;
AVG_WEATHER = FOREACH AVG_WEATHER_GRP GENERATE 
	'Chicago' as city,
	AVG($1.meanTemperature) as meanTemperature,
	AVG($1.meanVisibility) as meanVisibility,
	AVG($1.meanWindSpeed) as meanWindSpeed,
	COUNT_STAR($1) as count,
	SUM($1.fog) as fog,
	SUM($1.rain) as rain,
	SUM($1.snow) as snow,
	SUM($1.hail) as hail,
	SUM($1.thunder) as thunder,
	SUM($1.tornado) as tornado,
	SUM($1.clear) as clear;

STORE AVG_WEATHER INTO 'hbase://ave_chicago_weather'
  USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('
    ave:meanTemperature, ave:meanVisibility, ave:meanWindSpeed,
    ave:count, ave:fog, ave:rain, ave:snow, ave:hail, ave:thunder,
    ave:tornado, ave:clear');
