package flix2.stormkafka.bolt;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import flix2.stormkafka.mysql.MySqlTimeSpent;
import flix2.stormkafka.mysql.MySqlTopView;

public class TimeSpentErrorEventBolt extends BaseRichBolt {

	final static Logger logger = Logger.getLogger(TimeSpentErrorEventBolt.class);
	OutputCollector _collector;

	public TimeSpentErrorEventBolt() {

	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this._collector = collector;
	}

	@Override
	public void execute(Tuple tutple) {
		try {
			String action = tutple.getStringByField("action");
			String time_spent = tutple.getStringByField("time_spent");
			String total_time = tutple.getStringByField("total_time");
			boolean isUpdateTimeSpent = (action != null && action != "" && action.equals("exit"));
			boolean isError = (total_time == null || total_time == "") || (time_spent == null || time_spent == "");
			if (isUpdateTimeSpent && isError) {
				// new
				String user_id = tutple.getStringByField("user_id");
				String profile_id = tutple.getStringByField("profile_id");
				String device_id = tutple.getStringByField("device_id");
				String kind = tutple.getStringByField("kind");
				String parent_id = tutple.getStringByField("parent_id");
				String content_id = tutple.getStringByField("content_id");
				String timeServerStr = tutple.getStringByField("timeServer");
				long timeServer = (long) (Float.parseFloat(timeServerStr) * 1000L);
				String timeClientStr = tutple.getStringByField("timeClient");
				long timeClient = (long) (Float.parseFloat(timeClientStr));
				this._collector.emit(tutple, new Values(kind, action, user_id, parent_id, profile_id, timeServer,
						timeClient,content_id, device_id, total_time, time_spent));
			}
			logger.debug("ack tuple : " + tutple.toString());
			this._collector.ack(tutple);
		} catch (Exception e) {
			logger.error("Error when emit tuple : "  + e.getMessage());
			this._collector.fail(tutple);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(new String[] { "kind", "action", "user_id", "parent_id", "profile_id",
				"timeServer","timeClient",
				"content_id", "device_id", "total_time", "time_spent" }));

	}

}
