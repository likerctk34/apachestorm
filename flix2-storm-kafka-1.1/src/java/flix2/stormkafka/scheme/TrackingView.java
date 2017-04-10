package flix2.stormkafka.scheme;

import java.sql.Timestamp;

import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TrackingView extends StringSerializer {

	public TrackingView(){
		
	}
	public String getUser_id() {
		return user_id;
	}

	public void setUser_id(String user_id) {
		this.user_id = user_id;
	}

	public String getProfile_id() {
		return profile_id;
	}

	public void setProfile_id(String profile_id) {
		this.profile_id = profile_id;
	}

	public String getDevice_id() {
		return device_id;
	}

	public void setDevice_id(String device_id) {
		this.device_id = device_id;
	}

	public String getContent_id() {
		return content_id;
	}

	public void setContent_id(String content_id) {
		this.content_id = content_id;
	}

	public String getParent_id() {
		return parent_id;
	}

	public void setParent_id(String parent_id) {
		this.parent_id = parent_id;
	}

	public String getKind() {
		return kind;
	}

	public void setKind(String kind) {
		this.kind = kind;
	}

	public String getDuration() {
		return duration;
	}

	public void setDuration(String duration) {
		this.duration = duration;
	}

	public String getPlatform() {
		return platform;
	}

	public void setPlatform(String platform) {
		this.platform = platform;
	}

	public Timestamp getUpdated() {
		return updated;
	}

	public void setUpdated(Timestamp updated) {
		this.updated = updated;
	}

	public String getTotal_time() {
		return total_time;
	}

	public void setTotal_time(String total_time) {
		this.total_time = total_time;
	}

	public String getTimeServer() {
		return timeServer;
	}

	public void setTimeServer(String timeServer) {
		this.timeServer = timeServer;
	}

	String total_time;
	String profile_id = "";
	String user_id = "";
	String device_id = "";
	String content_id = "";
	String parent_id;
	String kind;
	String duration;
	String platform;
	Timestamp updated;

	@JsonProperty("time.srv")
	String timeServer = "";

}
