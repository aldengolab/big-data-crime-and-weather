CRIMES = LOAD '/inputs/chicagoCrimeTypes' USING PigStorage(',') 
	as (year, month:int, day:int, hour:int, primaryType,
	meanTemperature:double, fog:long, rain:long, snow:long, 
	hail:long, thunder:long, tornado:long, clear:long);

GRP_CRIME = GROUP CRIMES BY (primaryType, year);
CRIME_BY_YEAR = FOREACH GRP_CRIME GENERATE
	CONCAT(group.primaryType, '-', group.year) as crimeyear, 
	COUNT_STAR($1) as count;

STORE CRIME_BY_YEAR INTO 'hbase://crime_year_sums'
  USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(
    'count:sum');