/*

import java.util.Properties;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import flix2.stormkafka.bolt.TopViewEventBolt;
import flix2.stormkafka.bolt.TrackingViewBoltold;
import flix2.stormkafka.mysql.HikariDataSourceProvider;
import flix2.stormkafka.scheme.EventPlayScheme;
import flix2.stormkafka.scheme.TrackingViewScheme;
import flix2.stormkafka.starter.HikariDataSourceProviderTest;

public class Flix2TopologyTest {

	public static void main(String[] args) {

		BrokerHosts hosts = new ZkHosts("localhost:2181");

		
		// for topic flix2-event
		SpoutConfig spoutKafka_flix2_event_Config = new SpoutConfig(hosts, "flix2-event", "/" + "flix2-event",
				UUID.randomUUID().toString());
		ObjectMapper mapper2 = new ObjectMapper();
		EventPlayScheme topViewScheme = new EventPlayScheme(mapper2);
		mapper2.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		spoutKafka_flix2_event_Config.scheme = new SchemeAsMultiScheme(topViewScheme);
		spoutKafka_flix2_event_Config.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
		KafkaSpout kafka_flix2_event_Spout = new KafkaSpout(spoutKafka_flix2_event_Config);

		TopologyBuilder builder = new TopologyBuilder();
		//set spouts
		builder.setSpout("kafka-flix2-event-spout", kafka_flix2_event_Spout, 200);
		//set bolts
		HikariDataSourceProviderTest dataSourceProvider = new HikariDataSourceProviderTest();
		builder.setBolt("kafka-flix2-event-bolt", new TopViewBoltTest(dataSourceProvider), 200)
		.shuffleGrouping("kafka-flix2-event-spout");
		
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("TopViewCluster", new Config(), builder.createTopology());

	}

}
*/