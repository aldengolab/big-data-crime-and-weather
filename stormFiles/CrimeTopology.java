package agolab.CrimeWeatherApp.CrimeTopology;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class CrimeTopology {

	static class ProcessRawReport extends BaseBasicBolt {
		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			String report = tuple.getString(0);
			String[] values = report.split(",");
			if (values.length == 14 && values[4] != "empty") {
				int month = (int) Integer.parseInt(values[0]);
				int day = (int) Integer.parseInt(values[1]);
				int year = (int) Integer.parseInt(values[2]);
				int hour = (int) Integer.parseInt(values[3]);
				String crime = values[4];
				String neighborhood = values[5];
				double temp = Double.parseDouble(values[6]);
				int fog = (int) Integer.parseInt(values[7]);
				int rain = (int) Integer.parseInt(values[8]);
				int snow = (int) Integer.parseInt(values[9]);
				int hail = (int) Integer.parseInt(values[10]);
				int thunder = (int) Integer.parseInt(values[11]);
				int tornado = (int) Integer.parseInt(values[12]);
				int clear = (int) Integer.parseInt(values[13]);
				collector.emit(new Values(month, day, year, crime, neighborhood, temp, fog, rain, snow, hail, thunder, tornado, clear));
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("month", "day", "year", "hour", "crime", "neighborhood", "temperature", "fog", "rain", "snow", "hail", "thunder", "tornado", "clear"));
		}

	}

	static class UpdateHbaseBolt extends BaseBasicBolt {
		private org.apache.hadoop.conf.Configuration conf;
		private Connection hbaseConnection;
		@Override
		public void prepare(Map stormConf, TopologyContext context) {
			try {
				conf = HBaseConfiguration.create();
			    conf.set("hbase.zookeeper.property.clientPort", "2181");
			    conf.set("hbase.zookeeper.quorum", StringUtils.join((List<String>)(stormConf.get("storm.zookeeper.servers")), ","));
			    String znParent = (String)stormConf.get("zookeeper.znode.parent");
			    if(znParent == null)
			    	znParent = new String("/hbase");
				conf.set("zookeeper.znode.parent", znParent);
				hbaseConnection = ConnectionFactory.createConnection(conf);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			super.prepare(stormConf, context);
		}

		@Override
		public void cleanup() {
			try {
				hbaseConnection.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// TODO Auto-generated method stub
			super.cleanup();
		}

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			try {
				Table table = hbaseConnection.getTable(TableName.valueOf("agolab_summed_crimes_by_community"));
				String key = String.join(",", input.getStringByField("neighborhood"), input.getStringByField("year"));
				String crime = input.getStringByField("crime");
				Increment increment = new Increment(Bytes.toBytes(key));
				increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("crime_count"), 1);
				if (crime == "HOMICIDE") {
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("num_homicides"), 1);
				}
				if (crime == "ROBBERY") {
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("num_robberies"), 1);
				}
				if (crime == "BATTERY") {
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("num_batteries"), 1);
				}
				if (crime == "ASSAULT") {
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("num_assaults"), 1);
				}
				if (crime == "BURGLARY") {
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("num_burglaries"), 1);
				}
				if (crime == "THEFT") {
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("num_thefts"), 1);
				}
				if (crime == "NARCOTICS") {
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("num_narcotics"), 1);
				}
				if (crime == "OTHER") {
					increment.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("num_othert"), 1);
				}
				table.increment(increment);
				table.close();
				
				Table tableTwo = hbaseConnection.getTable(TableName.valueOf("agolab_crime_weather_sums"));
				Increment incrementTwo = new Increment(Bytes.toBytes(crime));
				int fog = input.getIntegerByField("fog");
				int rain= input.getIntegerByField("rain");
				int snow= input.getIntegerByField("snow");
				int hail = input.getIntegerByField("hail");
				int thunder = input.getIntegerByField("thunder");
				int tornado = input.getIntegerByField("tornado");
				int clear = input.getIntegerByField("clear");
				incrementTwo.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("count"), 1);
				incrementTwo.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("fog"), fog);
				incrementTwo.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("rain"), rain);
				incrementTwo.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("snow"), snow);
				incrementTwo.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("hail"), hail);
				incrementTwo.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("thunder"), thunder);
				incrementTwo.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("tornado"), tornado);
				incrementTwo.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("clear"), clear);
				tableTwo.increment(incrementTwo);
				tableTwo.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub

		}

	}

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		Map stormConf = Utils.readStormConfig();
		String zookeepers = StringUtils.join((List<String>)(stormConf.get("storm.zookeeper.servers")), ",");
		System.out.println(zookeepers);
		ZkHosts zkHosts = new ZkHosts(zookeepers);
		
		SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, "agolab", "/agolab","agolab_id");
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		// kafkaConfig.zkServers = (List<String>)stormConf.get("storm.zookeeper.servers");
		kafkaConfig.zkRoot = "/agolab";
		// kafkaConfig.zkPort = 2181;
		KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("agolab", kafkaSpout, 1);
		builder.setBolt("fields", new ProcessRawReport(), 1).shuffleGrouping("agolab");
		builder.setBolt("update-hbase", new UpdateHbaseBolt(), 1).shuffleGrouping("fields");

		Map conf = new HashMap();
		conf.put(backtype.storm.Config.TOPOLOGY_WORKERS, 4);

		if (args != null && args.length > 0) {
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		}   else {
			conf.put(backtype.storm.Config.TOPOLOGY_DEBUG, true);
			LocalCluster cluster = new LocalCluster(zookeepers, 2181L);
			cluster.submitTopology("weather-topology", conf, builder.createTopology());
		} 
	} 
}
