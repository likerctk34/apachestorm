package flix2.stormkafka.scheme;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import com.fasterxml.jackson.databind.ObjectMapper;

public class EventPlayScheme implements Scheme {
	
	
	final static Logger logger = Logger.getLogger(EventPlayScheme.class);
	private static final long serialVersionUID = 1L;
	private ObjectMapper _mapper;

	public EventPlayScheme(ObjectMapper mapper) {
		this._mapper = mapper;
		// TODO Auto-generated constructor stub
	}
	@Override
	public List<Object> deserialize(ByteBuffer ser) {
		EventPlay topView = null;
		try {
			topView = this._mapper.readValue(StringScheme.deserializeString(ser), EventPlay.class);
			 //MemoryMeter meter = new MemoryMeter();
			 //long size =  meter.measureDeep(topView);
			 //logger.info("mesg size : " + size);
		} catch (Exception e) {
			logger.error(e.getMessage());
		} 
		// TODO Auto-generated method stub
		return new Values(
				topView.getKind(),topView.getUser_id(), topView.getTimeClient(),
				topView.getParent_id(),topView.getIp(),topView.getProfile_id(),
				topView.getTimeServer(),topView.getUser_agent(), topView.getPlatform(),
				topView.getAction(),topView.getContent_id(), topView.getDevice_id(), 
				topView.getTime_spent() , topView.getTotal_time()
				);
	}

	@Override
	public Fields getOutputFields() {
		return new Fields(new String[] {
				"kind", "user_id", "timeClient", 
				"parent_id", "ip", "profile_id", 
				"timeServer", "user_agent", "platform",
				"action", "content_id", "device_id","time_spent","total_time"
				});
	}

}
