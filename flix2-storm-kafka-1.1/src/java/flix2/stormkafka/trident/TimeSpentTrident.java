package flix2.stormkafka.trident;

import org.apache.storm.tuple.Values;

import java.sql.Date;
import java.text.SimpleDateFormat;

import org.apache.log4j.Logger;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

public class TimeSpentTrident extends BaseFunction {

	final static Logger logger = Logger.getLogger(TimeSpentTrident.class);

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		try {
			String action = tuple.getStringByField("action");
			String time_spent = tuple.getStringByField("time_spent");
			String total_time = tuple.getStringByField("total_time");
			boolean isUpdateTimeSpent = (action != null && action != "" && action.equals("exit"));
			boolean isOldVersion = (total_time == null || total_time == "") && (time_spent == null || time_spent == "");
			if (isUpdateTimeSpent && !isOldVersion) {
				// new
				String user_id = tuple.getStringByField("user_id");
				String profile_id = tuple.getStringByField("profile_id");
				String device_id = tuple.getStringByField("device_id");
				String kind = tuple.getStringByField("kind");
				String parent_id = tuple.getStringByField("parent_id");
				String content_id = tuple.getStringByField("content_id");
				String timeServerStr = tuple.getStringByField("timeServer");
				long timeServer = (long) (Float.parseFloat(timeServerStr) * 1000L);
				timeServerStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(timeServer);
				String timeClientStr = tuple.getStringByField("timeClient");
				long timeClient = (long) (Float.parseFloat(timeClientStr));
				collector.emit(new Values(kind, action, user_id, parent_id, profile_id, timeServerStr, timeClient,
						content_id, device_id, total_time, time_spent));
			}
			logger.debug("ack tuple : " + tuple.toString());
		} catch (Exception e) {
			logger.error("Error when emit tuple : " + e.getMessage());
		}
	}

}
