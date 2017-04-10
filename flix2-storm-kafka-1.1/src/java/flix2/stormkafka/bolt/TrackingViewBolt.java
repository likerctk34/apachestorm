package flix2.stormkafka.bolt;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class TrackingViewBolt extends BaseRichBolt {

	final static Logger logger = Logger.getLogger(TrackingViewBolt.class);
	OutputCollector _collector;

	public TrackingViewBolt() {

	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this._collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		try {
			String kind = tuple.getStringByField("kind");
			boolean isUpdateTracking = (kind != null) && (kind.equals("v") || kind.equals("c"));
			if (isUpdateTracking) {

				String user_id = tuple.getStringByField("user_id");
				String profile_id = tuple.getStringByField("profile_id");
				String device_id = tuple.getStringByField("device_id");
				String content_id = tuple.getStringByField("content_id");
				String timeServer = tuple.getStringByField("timeServer");
				String parent_id = tuple.getStringByField("parent_id");
				String platform = tuple.getStringByField("platform");
				String duration = tuple.getStringByField("duration");
				String total_time = tuple.getStringByField("total_time");
				this._collector.emit(tuple, new Values(user_id, profile_id, device_id, content_id, parent_id, kind,
						duration, platform, timeServer,total_time));
			}
			this._collector.ack(tuple);

		} catch (Exception e) {
			logger.error(e.getMessage());
			this._collector.fail(tuple);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(new String[] { "user_id", "profile_id", "device_id", "content_id", "parent_id",
				"kind", "duration", "platform", "timeServer" ,"total_time"}));
	}

}
