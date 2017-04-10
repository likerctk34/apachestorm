package flix2.stormkafka.bolt;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseStatefulBolt;
import org.apache.storm.tuple.Tuple;

import flix2.stormkafka.starter.Flix2Config;

public class ErrorLogSendMsgBolt extends BaseStatefulBolt<KeyValueState<String, ArrayList<Tuple>>> {

	private KeyValueState<String, ArrayList<Tuple>> totalErrorState;
	private OutputCollector collector;
	private int timeIntelval = 5;
	private long lastCheck;
	private String token="";

	final static Logger logger = Logger.getLogger(ErrorLogSendMsgBolt.class);

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		token = stormConf.get(Flix2Config.FLIX2_TRACKING_ERROR_LOG_SLACK_TOKEN).toString();

		Calendar cal = Calendar.getInstance();
		lastCheck =  cal.getTime().getTime()/1000;
	}

	@Override
	public void execute(Tuple input) {
		String kind = input.getStringByField("kind");
		String channel = input.getStringByField("channel");
		ArrayList<Tuple> errors = totalErrorState.get(kind, new ArrayList<Tuple>());
		errors.add(input);
		Calendar cal = Calendar.getInstance();
		long curTime =  cal.getTime().getTime()/1000;
		boolean isClear=false;
		if ( (curTime - lastCheck ) >= timeIntelval ) {
			// send msg to inform total api token error from earliest time to
			// latest time
			isClear=true;
			Collections.sort(errors, new Comparator<Tuple>() {
		        @Override
		        public int compare(Tuple t1, Tuple t2)
		        {
		            return  t1.getStringByField("timeServer")
		            		.compareTo(t2.getStringByField("timeServer"));
		        }
		    });
			String earlierTimeStr = errors.get(0).getStringByField("timeServer");
			String laterTimeStr = errors.get(errors.size()-1).getStringByField("timeServer");
			Date earlierTime = new Date((long) Float.parseFloat(earlierTimeStr) * 1000L);
			Date laterTime = new Date((long) Float.parseFloat(laterTimeStr) * 1000L);
			String earlierDatefmStr = new SimpleDateFormat("dd/MM/yyyy").format(earlierTime);
			String earlierTimefmStr = new SimpleDateFormat("HH:mm:ss").format(earlierTime);
			String laterTimefmStr = new SimpleDateFormat("HH:mm:ss").format(laterTime);
			String name = input.getStringByField("name");
			String msg = String.format("`%s %s->%s : %s`", earlierDatefmStr, earlierTimefmStr,laterTimefmStr,name);
			String listError = "\n";
			for (Tuple tuple : errors) {
				//Date date = new Date((long) Float.parseFloat(tuple.getStringByField("timeServer")) * 1000L);
				//String dateStr = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format(date);
				listError += "*ip*: " + tuple.getStringByField("ip");
				listError += " ,*device_id*: " + tuple.getStringByField("device_id");
				//istError += " ,time: " + dateStr;
				if (tuple.getStringByField("detailError") != null && tuple.getStringByField("detailError") != "")
					listError += " ," + tuple.getStringByField("detailError") + "\n";
				else
					listError += "\n";
			}
			logger.info(msg + listError);
			try {
				sendGet(msg + listError,this.token,channel);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//reset last time
			lastCheck = curTime;

		}
		if(isClear){
			errors.clear();
		}
		else{
			totalErrorState.put(kind, errors);
		}
		
		collector.ack(input);
		/*curTime =  cal.getTime().getTime()/1000;
		if ( isConnectionError && (curTime - lastCheck) >= timeIntelval ) {
			// send msg to inform total connection error from earliest time to
			// latest time
			isClear=true;
			Collections.sort(errors, new Comparator<Tuple>() {
		        @Override
		        public int compare(Tuple t1, Tuple t2)
		        {
		            return  t1.getStringByField("timeServer")
		            		.compareTo(t2.getStringByField("timeServer"));
		        }
		    });
			String earlierTimeStr = errors.get(0).getStringByField("timeServer");
			String laterTimeStr = errors.get(errors.size()-1).getStringByField("timeServer");
			Date earlierTime = new Date((long) Float.parseFloat(earlierTimeStr) * 1000L);
			String earlierDatefmStr = new SimpleDateFormat("dd/MM/yyyy").format(earlierTime);
			String earlierTimefmStr = new SimpleDateFormat("HH:mm:ss").format(earlierTime);
			Date laterTime = new Date((long) Float.parseFloat(laterTimeStr) * 1000L);
			String laterTimefmStr = new SimpleDateFormat("HH:mm:ss").format(laterTime);
			String msg = String.format("%d connection error from %s to %s", errors.size(),earlierTimefmStr,laterTimefmStr);
			String listError = "\nList connection errors :\n";
			for (Tuple tuple : errors) {
				Date date = new Date((long) Float.parseFloat(tuple.getStringByField("timeServer")) * 1000L);
				String dateStr = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format(date);
				listError += "ip: " + tuple.getStringByField("ip");
				listError += " ,device_id: " + tuple.getStringByField("device_id");
				listError += " ,time: " + dateStr;
				listError += " ,link: " +  tuple.getStringByField("detailError") + "\n";
			}
			logger.info(msg + listError);
			try {
				boolean isAPIS = (kind != null) && kind.matches("(.*)apis(.*)");
				if(isAPIS){
					
					sendGet(msg + listError,this.token, this.channel_linkapis);
				
				}else{
					
					sendGet(msg + listError,this.token, this.channel_linktv);
				}
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//reset last time
			lastCheck = curTime;
		}*/
		
	}
	
	private void sendGet(String msg,String token,String channel) throws Exception {
		msg = URLEncoder.encode(msg);
		String url = "https://slack.com/api/chat.postMessage?token="+
		token +"&channel=" + channel+ "&text="+ msg+ "&username=Flix2ErrorLog&pretty=1";

		HttpClient client = new DefaultHttpClient();
		HttpGet request = new HttpGet(url);

		// add request header
		request.addHeader("User-Agent", "Mozilla/5.0");

		HttpResponse response = client.execute(request);

		BufferedReader rd = new BufferedReader(
                       new InputStreamReader(response.getEntity().getContent()));

		StringBuffer result = new StringBuffer();
		String line = "";
		while ((line = rd.readLine()) != null) {
			result.append(line);
		}

		logger.info(result.toString());

	}

	@Override
	public void initState(KeyValueState<String, ArrayList<Tuple>> state) {
		this.totalErrorState = state;

	}

}
