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

import flix2.stormkafka.bolt.ErrorLogBolt;
import flix2.stormkafka.bolt.ErrorLogSendMsgBolt;
import flix2.stormkafka.bolt.TrackingViewBolt;
import flix2.stormkafka.mysql.HikariDataSourceProvider;
import flix2.stormkafka.scheme.ErrorLogScheme;
import flix2.stormkafka.scheme.ErrorLogView;
import flix2.stormkafka.scheme.TrackingViewScheme;
import org.apache.commons.cli.*;

public class Flix2ErrorLogTopology {

	public static String FLIX2_ERROR_LOG_SPOUT = "flix2-errorlog-spout";
	public static String FLIX2_ERROR_LOG_BOLT = "flix2-errorlog-bolt";
	public static String FLIX2_SEND_MSG_ERROR_LOG_BOLT = "flix2-send-msg-errorlog-bolt";
	public static int FLIX2_ERROR_LOG_PARALLELISMHINT = 5;

	public static void main(String[] args) {

		BrokerHosts hosts = new ZkHosts("localhost:2181");
		Map clusterConf = Utils.readStormConfig();
		// for topic flix2-event
		SpoutConfig spoutKafka_error_log_Config = new SpoutConfig(hosts, "flix2-errorlog", "/" + "flix2-errorlog",
				"errorlogFlix2stormkafka");
		ObjectMapper mapper = new ObjectMapper();
		ErrorLogScheme errorLogScheme = new ErrorLogScheme(mapper);
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		spoutKafka_error_log_Config.scheme = new SchemeAsMultiScheme(errorLogScheme);
		spoutKafka_error_log_Config.startOffsetTime =kafka.api.OffsetRequest.LatestTime();
		KafkaSpout kafka_error_log_Spout = new KafkaSpout(spoutKafka_error_log_Config);

		TopologyBuilder builder = new TopologyBuilder();
		// set spout
		FLIX2_ERROR_LOG_PARALLELISMHINT = Integer
				.parseInt(clusterConf.get(Flix2Config.FLIX2_TRACKING_ERROR_LOG_PARALLELISMHINT).toString());
		builder.setSpout(FLIX2_ERROR_LOG_SPOUT, kafka_error_log_Spout, FLIX2_ERROR_LOG_PARALLELISMHINT);

		// set bolt
		builder.setBolt(FLIX2_ERROR_LOG_BOLT, new ErrorLogBolt(), FLIX2_ERROR_LOG_PARALLELISMHINT)
				.shuffleGrouping(FLIX2_ERROR_LOG_SPOUT);

		builder.setBolt(FLIX2_SEND_MSG_ERROR_LOG_BOLT, new ErrorLogSendMsgBolt(),
				FLIX2_ERROR_LOG_PARALLELISMHINT).shuffleGrouping(FLIX2_ERROR_LOG_BOLT);

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
				StormSubmitter.submitTopology("ERRORLOGCluster", new Config(), builder.createTopology());
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
			cluster.submitTopology("ERRORLOGCluster", new Config(), builder.createTopology());

		}

	}
}
