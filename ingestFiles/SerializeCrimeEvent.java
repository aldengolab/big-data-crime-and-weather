package agolab.CrimeWeatherApp.IngestCrime;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;

import agolab.CrimeAndWeatherApp.crimeEvent.CrimeEvent;

public class SerializeCrimeEvent {
	static TProtocol protocol;
	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			conf.addResource(new Path("/home/mpcs53013/hadoop/etc/hadoop/core-site.xml"));
			conf.addResource("/home/mpcs53013/hadoop/etc/hadoop/hdfs-site.xml");
			System.setProperty("hadoop.home.dir", "/");
			final Configuration finalConf = new Configuration(conf);
			final TSerializer ser = new TSerializer(new TBinaryProtocol.Factory());
			CrimeEventProcessor processor = new CrimeEventProcessor() {
				Map<Integer, SequenceFile.Writer> yearMap = new HashMap<Integer, SequenceFile.Writer>();
				
				Writer getWriter(long yearlong) throws IOException {
					int year = (int) yearlong;
					// Chicago crime file is too small for vertical partitioning
					// Use this code to execute vertical partitioning if segmented files are large
					if (!yearMap.containsKey(1)) {
					yearMap.put(1, 
							SequenceFile.createWriter(finalConf,
									SequenceFile.Writer.file(
											new Path("/agolab/inputs/thriftCrime/chicagoCrime")),
									SequenceFile.Writer.keyClass(IntWritable.class),
									SequenceFile.Writer.valueClass(BytesWritable.class),
									SequenceFile.Writer.compression(CompressionType.NONE)));
					}
					return yearMap.get(1);
				}

				@Override
				void processCrimeEvent(CrimeEvent event, File file) throws IOException {
					try {
						getWriter(event.year).append(new IntWritable(1), new BytesWritable(ser.serialize(event)));;
					} catch (TException e) {
						throw new IOException(e);
					}
				}
			};
			processor.processCrimeFile(args[0]);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
