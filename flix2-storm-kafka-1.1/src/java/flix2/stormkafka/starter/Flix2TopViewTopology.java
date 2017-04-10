package flix2.stormkafka.starter;

import java.sql.Types;
import java.util.List;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
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

import flix2.stormkafka.bolt.TopViewEventBolt;
import flix2.stormkafka.mysql.HikariDataSourceProvider;
import flix2.stormkafka.mysql.QueryString;
import flix2.stormkafka.scheme.EventPlayScheme;

public class Flix2TopViewTopology {

	public static String FLIX2_EVENT_SPOUT = "flix2-event-spout";
	public static String FLIX2_TOP_VIEW_BOLT = "flix2-top-view-bolt";

	public static String FLIX2_INSERT_TOP_VIEW_BOLT = "flix2-top-insert-view-bolt";

	public static void main(String[] args) {

		BrokerHosts hosts = new ZkHosts("localhost:2181");

		// for topic flix2-event
		SpoutConfig spoutKafka_flix2_event_Config = new SpoutConfig(hosts, "flix2-event", "/" + "flix2-event",
				"flix2stormkafka");
		ObjectMapper mapper = new ObjectMapper();
		EventPlayScheme eventPlayScheme = new EventPlayScheme(mapper);
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		spoutKafka_flix2_event_Config.scheme = new SchemeAsMultiScheme(eventPlayScheme);
		spoutKafka_flix2_event_Config.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
		KafkaSpout kafka_flix2_event_Spout = new KafkaSpout(spoutKafka_flix2_event_Config);

		TopologyBuilder builder = new TopologyBuilder();
		// set spouts
		builder.setSpout(FLIX2_EVENT_SPOUT, kafka_flix2_event_Spout, 5);

		// set bolt
		builder.setBolt(FLIX2_TOP_VIEW_BOLT, new  TopViewEventBolt(), 5).shuffleGrouping(FLIX2_EVENT_SPOUT);

		// create data source
		HikariDataSourceProvider dataSourceProvider = new HikariDataSourceProvider(
				"210.245.18.76","flix2",
				"flixuser","Y2NuxdOt!@#EUdjFSX");

		// top view bolt
		List<Column> schemaTopViewColumns = Lists.newArrayList(new Column("kind", Types.VARCHAR),
				new Column("content_id", Types.VARCHAR), new Column("day", Types.VARCHAR));
		JdbcMapper mapperTopView = new SimpleJdbcMapper(schemaTopViewColumns);
		JdbcInsertBolt updateTopViewBolt = new JdbcInsertBolt(dataSourceProvider, mapperTopView);
		updateTopViewBolt = updateTopViewBolt.withInsertQuery(QueryString.pro_update_top_view_query);
		updateTopViewBolt = updateTopViewBolt.withQueryTimeoutSecs(3000);

		builder.setBolt(FLIX2_INSERT_TOP_VIEW_BOLT, updateTopViewBolt,5).shuffleGrouping(FLIX2_TOP_VIEW_BOLT);

		
		/*try {
			StormSubmitter.submitTopology("TopViewTestCluster", new Config(), builder.createTopology());
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
		cluster.submitTopology("TopViewEarlierTimeClusterTest", new Config(), builder.createTopology());

	}
}
