kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic agolab_weather_reports
hbase shell ./kafka-hb-set.txt
