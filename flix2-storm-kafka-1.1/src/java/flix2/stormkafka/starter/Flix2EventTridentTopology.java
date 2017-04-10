package flix2.stormkafka.starter;

import java.sql.Types;
import java.util.List;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.jdbc.trident.state.JdbcState;
import org.apache.storm.jdbc.trident.state.JdbcStateFactory;
import org.apache.storm.jdbc.trident.state.JdbcUpdater;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.TransactionalTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

import flix2.stormkafka.mysql.HikariDataSourceProvider;
import flix2.stormkafka.mysql.QueryString;
import flix2.stormkafka.scheme.EventPlayScheme;
import flix2.stormkafka.trident.TopViewCount;
import flix2.stormkafka.trident.TimeSpentTrident;
import flix2.stormkafka.trident.TopViewTrident;
import org.apache.commons.cli.*;

public class Flix2EventTridentTopology {

	public static String FLIX2_EVENT_SPOUT = "flix2eventspout";
	public static int FLIX2_EVENT_PARALLELISMHINT = 20;

	private TransactionalTridentKafkaSpout createKafkaSpout() {
		ZkHosts hosts = new ZkHosts("localhost:2181");
		TridentKafkaConfig config = new TridentKafkaConfig(hosts, "flix2-event", "eventTrident");
		config.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
		ObjectMapper mapper = new ObjectMapper();
		EventPlayScheme eventPlayScheme = new EventPlayScheme(mapper);
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		config.scheme = new SchemeAsMultiScheme(eventPlayScheme);

		return new TransactionalTridentKafkaSpout(config);
	}

	private void addTopViewStream(TridentTopology tridentTopology, TransactionalTridentKafkaSpout kafkaFlix2EventSpout,
			HikariDataSourceProvider dataSourceProvider) {

		// jdbc
		List<Column> schemaTopViewColumns = Lists.newArrayList(new Column("newkind", Types.VARCHAR),
				new Column("newcontent_id", Types.VARCHAR), new Column("day", Types.VARCHAR),
				new Column("count", Types.INTEGER));
		JdbcMapper mapperTopView = new SimpleJdbcMapper(schemaTopViewColumns);
		JdbcState.Options options = new JdbcState.Options().withConnectionProvider(dataSourceProvider)
				.withMapper(mapperTopView).withInsertQuery(QueryString.pro_update_top_view_trident_query);

		JdbcStateFactory jdbcStateFactory = new JdbcStateFactory(options);

		TridentState state = addTopViewTridentState(tridentTopology, kafkaFlix2EventSpout, dataSourceProvider);
		Stream stream = state.newValuesStream();
		stream.each(new Fields("newkind", "newcontent_id", "day", "count"), new FilterNull()).partitionPersist(
				jdbcStateFactory, new Fields("newkind", "newcontent_id", "day", "count"), new JdbcUpdater(),
				new Fields());
	}

	private TridentState addTopViewTridentState(TridentTopology tridentTopology,
			TransactionalTridentKafkaSpout kafkaFlix2EventSpout, HikariDataSourceProvider dataSourceProvider) {
		return tridentTopology.newStream("topviewstate", kafkaFlix2EventSpout)
				.parallelismHint(FLIX2_EVENT_PARALLELISMHINT)
				.each(new Fields("action", "kind", "parent_id", "content_id", "timeServer"), new TopViewTrident(),
						new Fields("newkind", "newcontent_id", "day"))
				.groupBy(new Fields("newkind", "newcontent_id", "day"))
				.persistentAggregate(new MemoryMapState.Factory(), new TopViewCount(), new Fields("count"))
				.parallelismHint(FLIX2_EVENT_PARALLELISMHINT);
	}

	public StormTopology buildConsumerTopology() {
		// create data source
		HikariDataSourceProvider dataSourceProvider = new HikariDataSourceProvider("210.245.18.76", "flix2", "flixuser",
				"Y2NuxdOt!@#EUdjFSX");
		// dataSourceProvider.prepare();
		// create kafka spout for topic flix2-event
		TransactionalTridentKafkaSpout kafkaFlix2EventSpout = createKafkaSpout();

		// create trident topology
		TridentTopology tridentTopology = new TridentTopology();
		// add top view stream
		addTopViewStream(tridentTopology, kafkaFlix2EventSpout, dataSourceProvider);
		return tridentTopology.build();
	}

	public static void main(String[] args) {
		Map clusterConf = Utils.readStormConfig();
		String host = clusterConf.get(Flix2Config.FLIX2_JDBC_MYSQL_FLIX2DB_HOST).toString();
		String dbname = clusterConf.get(Flix2Config.FLIX2_JDBC_MYSQL_FLIX2DB_DBNAME).toString();
		String username = clusterConf.get(Flix2Config.FLIX2_JDBC_MYSQL_FLIX2DB_USERNAME).toString();
		String password = clusterConf.get(Flix2Config.FLIX2_JDBC_MYSQL_FLIX2DB_PASSWORD).toString();
		FLIX2_EVENT_PARALLELISMHINT = Integer
				.parseInt(clusterConf.get(Flix2Config.FLIX2_TRACKING_TOPVIEW_PARALLELISMHINT).toString());

		Flix2EventTridentTopology flix2_Trident_TopView_Topology = new Flix2EventTridentTopology();
		Config conf = new Config();
		conf.setNumWorkers(1);

		Options options = new Options();
		Option input = new Option("m", "mode", true, "mode of cluster: local|production");
		options.addOption(input);
		CommandLineParser parser = new BasicParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			System.out.println("parse arguments error : " + e.getMessage());
		}

		String mode = cmd.getOptionValue("mode");
		System.out.println("mode :" + mode);

		if (mode != null && mode.equals("production")) {

			try {
				StormSubmitter.submitTopology("Flix2EventCluster", conf,
						flix2_Trident_TopView_Topology.buildConsumerTopology());
			} catch (AlreadyAliveException e) { // TODO Auto-generated catch
												// block
				e.printStackTrace();
			} catch (InvalidTopologyException e) { // TODO
				e.printStackTrace();
			} catch (AuthorizationException e) { // TODO Auto-generated catch
													// block
				e.printStackTrace();
			}
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("Flix2EventClusterLocal", conf,
					flix2_Trident_TopView_Topology.buildConsumerTopology());

		}

	}
}
