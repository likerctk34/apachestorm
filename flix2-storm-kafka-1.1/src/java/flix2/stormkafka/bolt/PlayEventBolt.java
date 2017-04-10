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

public class PlayEventBolt extends BaseRichBolt {

	final static Logger logger = Logger.getLogger(TimeSpentEventBolt.class);
	OutputCollector _collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this._collector = collector;

	}

	@Override
	public void execute(Tuple tutple) {
		try {
			
			String action = tutple.getStringByField("action");
			boolean isUpdatePlayEvent = action != null && action != "" && action.equals("play");
			if (isUpdatePlayEvent) {
				String timeServerStr = tutple.getStringByField("timeServer");
				long timeServer = (long) (Float.parseFloat(timeServerStr) * 1000L);
				String timeClientStr = tutple.getStringByField("timeClient");
				long timeClient = (long) (Float.parseFloat(timeClientStr));
				this._collector.emit(tutple,
						new Values(tutple.getStringByField("kind"), 
								tutple.getStringByField("user_id"), 
								timeClient,
								tutple.getStringByField("parent_id"),
								tutple.getStringByField("ip"),
								tutple.getStringByField("profile_id"),
								timeServer,
								tutple.getStringByField("user_agent"), 
								tutple.getStringByField("platform"),
								tutple.getStringByField("action"),
								tutple.getStringByField("content_id"),
								tutple.getStringByField("device_id")));
				
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
		declarer.declare(new Fields(new String[] { "kind", "user_id", "timeClient", "parent_id", "ip", "profile_id",
				"timeServer", "user_agent", "platform", "action", "content_id", "device_id" }));

	}

}
