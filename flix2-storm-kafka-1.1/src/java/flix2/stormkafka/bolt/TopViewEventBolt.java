package flix2.stormkafka.bolt;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class TopViewEventBolt extends BaseRichBolt {

	final static Logger logger = Logger.getLogger(TopViewEventBolt.class);
	OutputCollector _collector;

	public TopViewEventBolt() {

	}
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this._collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		try {
			this._collector.ack(tuple);
			String action = tuple.getStringByField("action");
			String kind = tuple.getStringByField("kind");
			boolean isUpdateTopview = (kind.equals("v") || kind.equals("c")) && 
					(action != null && action != "" && action.equals("play"));
			if (isUpdateTopview) {
				String parent_id = tuple.getStringByField("parent_id");
				String content_id = tuple.getStringByField("content_id");
				String dateServerStr = tuple.getStringByField("timeServer");
				Date dateServer = new Date((long)(Float.parseFloat(dateServerStr) * 1000L));
				dateServerStr = new SimpleDateFormat("yyyy-MM-dd").format(dateServer);
				//
				if (parent_id != null && parent_id != "") {
					content_id = parent_id;
					if (kind != null && kind != "") {
						if (kind.equals("v")) {
							kind = "e";
						}

						if (kind.equals("c")) {
							kind = "t";
						}
					}
				}
				//
				this._collector.emit(tuple,
						new Values(kind, content_id, dateServerStr));
			}
		} catch (Exception e) {
			this._collector.fail(tuple);
		}
		// may be send message here
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(new String[] {  "kind",  "content_id","day" }));
	}

}
