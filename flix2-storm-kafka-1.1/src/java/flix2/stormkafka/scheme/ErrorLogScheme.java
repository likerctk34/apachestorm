package flix2.stormkafka.scheme;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ErrorLogScheme implements Scheme {
	
	
	final static Logger logger = Logger.getLogger(ErrorLogScheme.class);
	private static final long serialVersionUID = 1L;
	private ObjectMapper _mapper;

	public ErrorLogScheme(ObjectMapper mapper) {
		this._mapper = mapper;
		// TODO Auto-generated constructor stub
	}
	@Override
	public List<Object> deserialize(ByteBuffer ser) {
		ErrorLogView errorLog = null;
		try {
			errorLog = this._mapper.readValue(StringScheme.deserializeString(ser), ErrorLogView.class);
		} catch (Exception e) {
			logger.error(e.getMessage());
		} 
		// TODO Auto-generated method stub
		return new Values(errorLog.getTimeClient(), errorLog.getUser_id(), errorLog.getIp(),
				errorLog.getProfile_id(), errorLog.getTimeServer(), errorLog.getResponse(),
				errorLog.getParams(), errorLog.getHttp_status(), errorLog.getAction(),
				errorLog.getUser_agent(), errorLog.getMethod(), errorLog.getDevice_id());
	}

	@Override
	public Fields getOutputFields() {
		return new Fields(new String[] { "timeClient", "user_id", "ip", "profile_id", "timeServer", "response", "params",
				"http_status", "action", "user-agent", "method" , "device_id" });
	}

}
