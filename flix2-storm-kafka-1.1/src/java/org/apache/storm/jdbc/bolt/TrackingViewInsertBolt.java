package org.apache.storm.jdbc.bolt;

import java.sql.SQLException;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import flix2.stormkafka.mysql.MySqlTrackingView;
import flix2.stormkafka.scheme.TrackingViewScheme;

public class TrackingViewInsertBolt extends BaseRichBolt {

	final static Logger logger = Logger.getLogger(TrackingViewInsertBolt.class);
	private static final long serialVersionUID = 1L;
	private MySqlTrackingView mySqlInsertUpdate;
	private ConnectionProvider connectionProvider;
	OutputCollector _collector;

	public TrackingViewInsertBolt() {

	}

	public TrackingViewInsertBolt(ConnectionProvider _connectionProvider) {
		this.connectionProvider = _connectionProvider;
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this._collector = collector;
		connectionProvider.prepare();
		mySqlInsertUpdate = new MySqlTrackingView(connectionProvider);
	}

	@Override
	public void execute(Tuple tuple) {
		try {
			mySqlInsertUpdate.InsertOrUpdateToDB(tuple);
			this._collector.ack(tuple);
		} catch (Exception e) {
			logger.error(e.getMessage());
			this._collector.fail(tuple);
		}
		// may be send message here
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

	@Override
	public void cleanup() {

		connectionProvider.cleanup();
	}

}
