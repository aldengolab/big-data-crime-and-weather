# crime-and-weather-system
Final Project MPCS 53013 Big Data | A big data system to handle weather and crime data

### Summary
This repo implements a [lambda architecture](https://en.wikipedia.org/wiki/Lambda_architecture) for a large-scale application that takes realtime weather and crime data, ingests it, then automatically running views of the data for users. 

### Tools Implemented

- Assumes a Hadoop HDFS file system hosted on Google Cloud
- Apache Kakfa for Serving Layer data collection
- Apache Storm topology for Serving Layer ingestion
- Apache Thrift data structure for fact-based, schema-on read data storage
- Apache Pig for Batch Layer pre-computed view construction
- Apache HBase for pre-computed view storage and access

### What's Here

- ingestFiles contains the jars for ingestion
- javaFiles will contain the java files that are the core functionality of the ingestFiles jars (retrieving)
- set-up contains the necessary shell code for running various aspects of the system
- pigFiles contain all the Pig code for batch layer runs
- mvn and pig contain necessary jars for implementation
