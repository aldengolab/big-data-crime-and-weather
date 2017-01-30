REGISTER /home/mpcs53013/agolab/pig/piggybank-0.16.0.jar;
REGISTER /home/mpcs53013/agolab/mvn/elephant-bird-core-4.14.jar;
REGISTER /home/mpcs53013/agolab/mvn/elephant-bird-pig-4.14.jar;
REGISTER /home/mpcs53013/agolab/mvn/elephant-bird-hadoop-compat-4.14.jar;
REGISTER /home/mpcs53013/agolab/mvn/libthrift-0.9.0.jar;
register /home/mpcs53013/agolab/jars/IngestCrime-0.0.1-SNAPSHOT.jar;

COMMUNITY_NAMES_RAW = LOAD '/agolab/inputs/crimeGeo/comms_names.csv' 
 USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER');
COMMUNITY_NAMES = FOREACH COMMUNITY_NAMES_RAW GENERATE $0 as community,
 $1 as code;

DEFINE WSThriftBytesToTuple com.twitter.elephantbird.pig.piggybank.ThriftBytesToTuple('agolab.CrimeAndWeatherApp.crimeEvent.CrimeEvent');
RAW_CRIME = LOAD '/agolab/inputs/thriftCrime/chicagoCrime' USING org.apache.pig.piggybank.storage.SequenceFileLoader() as (key:long, value: bytearray);
CRIME_EVENTS = FOREACH RAW_CRIME GENERATE FLATTEN(WSThriftBytesToTuple(value));

CRIME_COMMS_RAW = JOIN COMMUNITY_NAMES by code, CRIME_EVENTS by CrimeEvent::communityNum;
CRIME_COMMS = FOREACH CRIME_COMMS_RAW GENERATE 
	CrimeEvent::year, CrimeEvent::month, CrimeEvent::day, CrimeEvent::hour, 
	CrimeEvent::primaryType, community;
CRIME_TYPES = FOREACH CRIME_COMMS GENERATE
	CrimeEvent::year, CrimeEvent::month, CrimeEvent::day, CrimeEvent::hour, community,
	(primaryType == 'HOMICIDE' ? 1L : 0L) AS homicide,
	(primaryType == 'ROBBERY' ?  1L : 0L) AS robbery,
	(primaryType == 'BATTERY' ? 1L : 0L) AS battery,
	(primaryType == 'ASSAULT' ? 1L : 0L) AS assault,
	(primaryType == 'BURGLARY' ? 1L : 0L) AS burglary,
	(primaryType == 'THEFT' ? 1L : 0L) AS theft,
	(primaryType == 'NARCOTICS' ? 1L : 0L) AS narcotics,
	(primaryType != 'HOMICIDE' 
		AND primaryType != 'ROBBERY' 
		AND primaryType != 'BATTERY'
		AND primaryType != 'ASSAULT'
		AND primaryType != 'BURGLARY' 
		AND primaryType != 'THEFT' 
		AND primaryType != 'NARCOTICS' ? 1L : 0L) AS other;

STORE CRIME_TYPES into '/agolab/inputs/chicagoCrimeTypes' USING PigStorage(',');
