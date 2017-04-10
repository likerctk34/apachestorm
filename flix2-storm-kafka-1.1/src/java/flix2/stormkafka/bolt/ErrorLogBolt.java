package flix2.stormkafka.bolt;

import java.net.URL;
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

import flix2.stormkafka.starter.Flix2Config;

public class ErrorLogBolt extends BaseRichBolt {

	final static Logger logger = Logger.getLogger(ErrorLogBolt.class);
	OutputCollector _collector;
	private String channel_linktv="";
	private String channel_linkapis="";
	private String channel_backendapi="";
	public ErrorLogBolt() {

	}
	

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this._collector = collector;
		channel_linktv = conf.get(Flix2Config.FLIX2_TRACKING_ERROR_LOG_SLACK_CHANNEL_LINKTV).toString();
		channel_linkapis = conf.get(Flix2Config.FLIX2_TRACKING_ERROR_LOG_SLACK_CHANNEL_LINKAPIS).toString();
		channel_backendapi = conf.get(Flix2Config.FLIX2_TRACKING_ERROR_LOG_SLACK_CHANNEL_BACKENDAPI).toString();
	}

	@Override
	public void execute(Tuple tuple) {
		this._collector.ack(tuple);
		try {
			String action = tuple.getStringByField("action");
			boolean isBackEndAPIError = ((action != null) && action.equals("TokenError"))
										|| ((action != null) && action.equals("get config"));
			if (isBackEndAPIError) {
				String timeClient = tuple.getStringByField("timeClient");
				String timeServer = tuple.getStringByField("timeServer");
				String device_id = tuple.getStringByField("device_id");
				String ip = tuple.getStringByField("ip");
				Object detailError = tuple.getValueByField("response");
				String kind = "backendAPI";
				this._collector.emit(tuple, new Values(kind,"Backend API error with kind : "+ action,channel_backendapi,timeClient,timeServer,device_id,ip,"*response* :" +detailError.toString()));
			}
			String http_status = tuple.getStringByField("http_status");
			String method = tuple.getStringByField("method");
			boolean isConnectionError = (http_status != null) && http_status.equals("0") 
					&& (method != null) && method.equals("GET");
			if (isConnectionError) {
				String timeClient = tuple.getStringByField("timeClient");
				String timeServer = tuple.getStringByField("timeServer");
				String device_id = tuple.getStringByField("device_id");
				String ip = tuple.getStringByField("ip");
				URL aURL = new URL(tuple.getStringByField("action"));
				String host = aURL.getHost();
				String kind = aURL.getHost() + "Connection";
				boolean isAPIS = (host != null) && host.matches("(.*)apis.flix.vn(.*)");
				String channel="";
				String link = "";
				if(isAPIS){
					//link = "";
					link ="*link* : " + tuple.getStringByField("action");
					channel = channel_linkapis;
				}else{
					link = "";
					channel = channel_linktv;
				}
				this._collector.emit(tuple, new Values(kind,"Cannot connect to " + aURL.getProtocol() +"://"+ aURL.getHost() ,channel,timeClient,timeServer,device_id,ip,link));
			}

		} catch (Exception e) {
			logger.error("tuple error : " + tuple.toString());
			logger.error(e.getMessage());
			this._collector.fail(tuple);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(new String[] { "kind","name","channel","timeClient","timeServer","device_id","ip","detailError"}));
	}

}
