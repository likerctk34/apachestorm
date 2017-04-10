package flix2.stormkafka.starter;

import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.jdbc.bolt.TrackingViewInsertBolt;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import flix2.stormkafka.bolt.TrackingViewBolt;
import flix2.stormkafka.mysql.HikariDataSourceProvider;
import flix2.stormkafka.scheme.TrackingViewScheme;
import org.apache.commons.cli.*;

public class Flix2TrackingViewTopology {

	public static String FLIX2_EVENT_SPOUT = "flix2-duration-spout";
	public static String FLIX2_TRACKING_VIEW_BOLT = "flix2-trackingview-bolt";
	public static String FLIX2_INSERT_TRACKING_VIEW_BOLT = "flix2-insert-trackingview-bolt";
	public static int FLIX2_TRACKING_PARALLELISMHINT = 20;

	public static void main(String[] args) {

		BrokerHosts hosts = new ZkHosts("localhost:2181");
		Map clusterConf = Utils.readStormConfig();
		// for topic flix2-event
		SpoutConfig spoutKafka_flix2_event_Config = new SpoutConfig(hosts, "flix2-duration", "/" + "flix2-duration",
				"flix2stormkafka");
		ObjectMapper mapper = new ObjectMapper();
		TrackingViewScheme trackingViewScheme = new TrackingViewScheme(mapper);
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		spoutKafka_flix2_event_Config.scheme = new SchemeAsMultiScheme(trackingViewScheme);
		spoutKafka_flix2_event_Config.startOffsetTime =kafka.api.OffsetRequest.LatestTime();
		KafkaSpout kafka_flix2_event_Spout = new KafkaSpout(spoutKafka_flix2_event_Config);

		TopologyBuilder builder = new TopologyBuilder();
		// set spout
		FLIX2_TRACKING_PARALLELISMHINT = Integer
				.parseInt(clusterConf.get(Flix2Config.FLIX2_TRACKING_DURATION_PARALLELISMHINT).toString());
		builder.setSpout(FLIX2_EVENT_SPOUT, kafka_flix2_event_Spout, FLIX2_TRACKING_PARALLELISMHINT);

		// create data source
		String host = clusterConf.get(Flix2Config.FLIX2_JDBC_MYSQL_FLIX2DB_HOST).toString();
		String dbname = clusterConf.get(Flix2Config.FLIX2_JDBC_MYSQL_FLIX2DB_DBNAME).toString();
		String username = clusterConf.get(Flix2Config.FLIX2_JDBC_MYSQL_FLIX2DB_USERNAME).toString();
		String password = clusterConf.get(Flix2Config.FLIX2_JDBC_MYSQL_FLIX2DB_PASSWORD).toString();
		HikariDataSourceProvider dataSourceProvider = new HikariDataSourceProvider(host, dbname, username, password);

		// set bolt
		builder.setBolt(FLIX2_TRACKING_VIEW_BOLT, new TrackingViewBolt(), FLIX2_TRACKING_PARALLELISMHINT)
				.fieldsGrouping(FLIX2_EVENT_SPOUT,
						new Fields(new String[] { "user_id", "profile_id", "device_id", "content_id" }));

		builder.setBolt(FLIX2_INSERT_TRACKING_VIEW_BOLT, new TrackingViewInsertBolt(dataSourceProvider),
				FLIX2_TRACKING_PARALLELISMHINT).fieldsGrouping(FLIX2_TRACKING_VIEW_BOLT,
						new Fields(new String[] { "user_id", "profile_id", "device_id", "content_id" }));

		Options options = new Options();
		Option input = new Option("m", "mode", true, "mode of cluster: local|production");
		options.addOption(input);
		CommandLineParser parser = new BasicParser();
		CommandLine cmd = null;
		try {
			System.out.println("args : " + args);
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			System.out.println("parse arguments error : "  + e.getMessage());
		}

		String mode = cmd.getOptionValue("mode");
		System.out.println("mode :" + mode);

		if (mode != null && mode.equals("production")) {

			try {
				StormSubmitter.submitTopology("TrackingViewCluster", new Config(), builder.createTopology());
			} catch (AlreadyAliveException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (AuthorizationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("TrackingViewClusterLocal", new Config(), builder.createTopology());

		}

	}
}
