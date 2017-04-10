package flix2.stormkafka.starter;

import java.sql.Types;
import java.util.List;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.bolt.TimeSpentInsertBolt;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

import flix2.stormkafka.bolt.PlayEventBolt;
import flix2.stormkafka.bolt.TimeSpentEventBolt;
import flix2.stormkafka.mysql.HikariDataSourceProvider;
import flix2.stormkafka.mysql.QueryString;
import flix2.stormkafka.scheme.EventPlayScheme;

public class Flix2PlayEventTopology {

	public static String FLIX2_EVENT_SPOUT = "flix2-event-spout";
	public static String FLIX2_PLAY_BOLT = "flix2-event-play-bolt";
	public static String FLIX2_INSERT_PLAY_BOLT = "flix2-event-insert-play-bolt";

	public static void main(String[] args) {

		BrokerHosts hosts = new ZkHosts("localhost:2181");

		// for topic flix2-event
		SpoutConfig spoutKafka_flix2_event_Config = new SpoutConfig(hosts, "flix2-event", "/" + "flix2-event",
				"flix2stormkafka");
		ObjectMapper mapper = new ObjectMapper();
		EventPlayScheme eventPlayScheme = new EventPlayScheme(mapper);
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		spoutKafka_flix2_event_Config.scheme = new SchemeAsMultiScheme(eventPlayScheme);
		spoutKafka_flix2_event_Config.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
		KafkaSpout kafka_flix2_event_Spout = new KafkaSpout(spoutKafka_flix2_event_Config);

		TopologyBuilder builder = new TopologyBuilder();
		// set spouts
		builder.setSpout(FLIX2_EVENT_SPOUT, kafka_flix2_event_Spout, 100);

		// event bolt
		builder.setBolt(FLIX2_PLAY_BOLT, new PlayEventBolt(), 100).shuffleGrouping(FLIX2_EVENT_SPOUT);

		// create data source
		HikariDataSourceProvider dataSourceProvider = new HikariDataSourceProvider(
				"210.245.18.114","flix2",
				"flixuser","Y2NuxdOt!@#EUdjFSX");

		// play event bolt
		List<Column> schemaPlayEventColumns = Lists.newArrayList(new Column("kind", Types.VARCHAR),
				new Column("user_id", Types.VARCHAR), new Column("timeClient", Types.TIMESTAMP),
				new Column("parent_id", Types.VARCHAR), new Column("ip", Types.VARCHAR),
				new Column("profile_id", Types.VARCHAR), new Column("timeServer", Types.TIMESTAMP),
				new Column("user_agent", Types.VARCHAR), new Column("platform", Types.VARCHAR),
				new Column("action", Types.VARCHAR), new Column("content_id", Types.VARCHAR),
				new Column("device_id", Types.VARCHAR));
		
		JdbcMapper mapperPlayEvent = new SimpleJdbcMapper(schemaPlayEventColumns);
		JdbcInsertBolt insertPlayEventBolt = new JdbcInsertBolt(dataSourceProvider, mapperPlayEvent);
		insertPlayEventBolt = insertPlayEventBolt.withInsertQuery(QueryString.Insert_flix_vod_play_events_query);
		insertPlayEventBolt = insertPlayEventBolt.withQueryTimeoutSecs(3000);
		
		builder.setBolt(FLIX2_INSERT_PLAY_BOLT, insertPlayEventBolt, 100).shuffleGrouping(FLIX2_PLAY_BOLT);

	/*	try {
			StormSubmitter.submitTopology("PlayEventClusterTest_100", new Config(), builder.createTopology());
		} catch (AlreadyAliveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AuthorizationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("PlayEventClusterTest_100", new Config(), builder.createTopology());

	}
}
