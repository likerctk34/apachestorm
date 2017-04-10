package flix2.stormkafka.scheme;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import com.fasterxml.jackson.databind.ObjectMapper;

public class TrackingViewScheme implements Scheme {
	
	
	final static Logger logger = Logger.getLogger(TrackingViewScheme.class);
	private static final long serialVersionUID = 1L;
	private ObjectMapper _mapper;

	public TrackingViewScheme(ObjectMapper mapper) {
		this._mapper = mapper;
		// TODO Auto-generated constructor stub
	}
	@Override
	public List<Object> deserialize(ByteBuffer ser) {
		TrackingView tracking = null;
		try {
			tracking = this._mapper.readValue(StringScheme.deserializeString(ser), TrackingView.class);
		} catch (Exception e) {
			logger.error(e.getMessage());
		} 
		// TODO Auto-generated method stub
		return new Values(tracking.getUser_id(), tracking.getTotal_time(), tracking.getKind(), tracking.getContent_id(),
				tracking.getDevice_id(), tracking.getParent_id(), tracking.getPlatform(), tracking.getProfile_id(),
				tracking.getDuration(), tracking.getTimeServer());
	}

	@Override
	public Fields getOutputFields() {
		return new Fields(new String[] { "user_id", "total_time", "kind", "content_id", "device_id", "parent_id",
				"platform", "profile_id", "duration", "timeServer" });
	}

}
