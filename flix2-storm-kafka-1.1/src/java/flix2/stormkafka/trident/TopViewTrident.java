package flix2.stormkafka.trident;
import org.apache.storm.tuple.Values;

import java.sql.Date;
import java.text.SimpleDateFormat;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
public class TopViewTrident extends BaseFunction {

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		try {
			String action = tuple.getStringByField("action");
			String kind = tuple.getStringByField("kind");
			boolean isUpdateTopview = (kind != null && (kind.equals("v") || kind.equals("c"))) && 
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
				collector.emit(new Values(kind, content_id, dateServerStr));
			}
		} catch (Exception e) {
			collector.reportError(e);
		}
		
	}

}
