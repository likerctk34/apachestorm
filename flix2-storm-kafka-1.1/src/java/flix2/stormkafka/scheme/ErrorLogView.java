package flix2.stormkafka.scheme;

import java.sql.Timestamp;

import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ErrorLogView extends StringSerializer {

	public ErrorLogView(){
		
	}

	public String getProfile_id() {
		return profile_id;
	}

	public void setProfile_id(String profile_id) {
		this.profile_id = profile_id;
	}

	public String getUser_id() {
		return user_id;
	}

	public void setUser_id(String user_id) {
		this.user_id = user_id;
	}

	public String getDevice_id() {
		return device_id;
	}

	public void setDevice_id(String device_id) {
		this.device_id = device_id;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public Object getResponse() {
		return response;
	}

	public void setResponse(Object response) {
		this.response = response;
	}

	public Object getParams() {
		return params;
	}

	public void setParams(Object params) {
		this.params = params;
	}

	public String getHttp_status() {
		return http_status;
	}

	public void setHttp_status(String http_status) {
		this.http_status = http_status;
	}

	public String getAction() {
		return action;
	}

	public void setAction(String action) {
		this.action = action;
	}

	public String getMethod() {
		return method;
	}

	public void setMethod(String method) {
		this.method = method;
	}

	public String getUser_agent() {
		return user_agent;
	}

	public void setUser_agent(String user_agent) {
		this.user_agent = user_agent;
	}

	public String getTimeServer() {
		return timeServer;
	}

	public void setTimeServer(String timeServer) {
		this.timeServer = timeServer;
	}

	public String getTimeClient() {
		return timeClient;
	}

	public void setTimeClient(String timeClient) {
		this.timeClient = timeClient;
	}

	String profile_id = "";
	String user_id = "";
	String device_id = "";
	String ip;
	Object response;
	Object params;
	String http_status;
	String action;
	
	String method;
	
	@JsonProperty("user-agent")
	String user_agent;
	
	@JsonProperty("time.srv")
	String timeServer = "";
	
	@JsonProperty("time.clt")
	String timeClient = "";

}
