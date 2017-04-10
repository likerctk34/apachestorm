package flix2.stormkafka.scheme;

import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.annotation.JsonProperty;

public class EventPlay extends StringSerializer {

	public String getContent_id() {
		return content_id;
	}

	public void setContent_id(String content_id) {
		this.content_id = content_id;
	}

	public String getKind() {
		return kind;
	}

	public void setKind(String kind) {
		this.kind = kind;
	}

	public String getTimeServer() {
		return timeServer;
	}

	public void setTimeServer(String timeServer) {
		this.timeServer = timeServer;
	}
	
	String kind;
	String user_id = "";
	
	@JsonProperty("time.clt")
	String timeClient = "";
	
	public String getUser_id() {
		return user_id;
	}

	public void setUser_id(String user_id) {
		this.user_id = user_id;
	}

	public String getTimeClient() {
		return timeClient;
	}

	public void setTimeClient(String timeClient) {
		this.timeClient = timeClient;
	}

	public String getParent_id() {
		return parent_id;
	}

	public void setParent_id(String parent_id) {
		this.parent_id = parent_id;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
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

	String parent_id = "" ;
	String ip = "";
	String profile_id = "";
	
	@JsonProperty("time.srv")
	String timeServer = "";
	
	String action = "";
	String content_id = "";
	public String getTotal_time() {
		return total_time;
	}

	public void setTotal_time(String total_time) {
		this.total_time = total_time;
	}

	public String getTime_spent() {
		return time_spent;
	}

	public void setTime_spent(String time_spent) {
		this.time_spent = time_spent;
	}

	String total_time = "";
	String time_spent = "";
	public String getPlatform() {
		return platform;
	}

	public void setPlatform(String platform) {
		this.platform = platform;
	}

	String device_id = "";
	String platform = "";
	
	@JsonProperty("user-agent")
	String user_agent = "";
	
	public String getUser_agent() {
		return user_agent;
	}

	public void setUser_agent(String user_agent) {
		this.user_agent = user_agent;
	}

	public String getAction() {
		return action;
	}

	public void setAction(String action) {
		this.action = action;
	}

}
