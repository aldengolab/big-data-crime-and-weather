# big-data-crime-and-weather
A big data system to handle weather and crime data | Final Project MPCS 53013 Big Data

### Summary
The code in this repo implements a [lambda architecture](https://en.wikipedia.org/wiki/Lambda_architecture) to feed a large-scale data application that takes weather and crime data, ingests it to HDFS, then automatically runs batch views of the data for user availability while also allowing for real-time updates on the fly.

You can see the Speed layer interface [here](http://104.197.248.161/agolab-crime-reports.html). Fair warning, it's not very pretty because of the time constraints placed on this project -- the effort was, necessarily, on the back end functionality.

### Tools Implemented

- Assumes a Hadoop HDFS file system hosted on Google Cloud
- Apache Kakfa for Serving Layer data collection
- Apache Storm topology for Serving Layer ingestion
- Apache Thrift data structure for fact-based, schema-on read data storage
- Apache Pig for Batch Layer pre-computed view construction
- Apache HBase for pre-computed view storage, Serving Layer data storage, and data access in Speed Layer
- Basic HTML with Python back-end for Speed Layer data access

### What's Here

- `set-up` contains the necessary shell code for running various aspects of the system
- `frontEnd` contains the Speed Layer for data access
- `ingestFiles` contains the ingestion code for HDFS serialization
- `thriftFiles` contains the Thrift schema for serialization
- `pigFiles` contain all the Pig code for batch layer runs
- `stormFiles` contains the code for the Serving layer Storm topology
- `mvn` and `pig` contain necessary open-source application jars for implementation

### Data

Data are from the NOAA (ftp://ftp.ncdc.noaa.gov/pub/data/gsod/) and the [City of Chicago data portal](https://data.cityofchicago.org/?browseSearch=crime).
