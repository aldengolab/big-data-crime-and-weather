CRIMES = LOAD '/agolab/inputs/chicagoCrimeTypes' USING PigStorage(',') 
	as (year, month:int, day:int, hour:int, community,
	homicide:long, robbery:long, battery:long, assault:long, burglary:long,
	theft:long, narcotics:long, other:long);

GRP_COMM = GROUP CRIMES by (community, year);
SUMMED_CRIMES_BY_COMM = FOREACH GRP_COMM GENERATE
	CONCAT(group.community, '-', group.year) as communityyear,
	COUNT_STAR($1) AS crime_count,
	SUM($1.homicide) AS num_homicides,
	SUM($1.robbery) AS num_robberies,
	SUM($1.battery) AS num_batteries,
	SUM($1.assault) AS num_assaults,
	SUM($1.burglary) AS num_burglaries,
	SUM($1.theft) AS num_thefts,
	SUM($1.narcotics) AS num_narcotics,
	SUM($1.other) AS num_other;

STORE SUMMED_CRIMES_BY_COMM INTO 'hbase://agolab_summed_crimes_by_community'
  USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(
    'crime:crime_count, crime:num_homicides, crime:num_robberies, 
     crime:num_batteries, crime:num_assaults, crime:num_burglaries, 
     crime:num_thefts, crime:num_narcotics, crime:num_other');

