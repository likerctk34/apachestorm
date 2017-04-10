package flix2.stormkafka.starter;
import org.apache.storm.Config;
import org.apache.storm.validation.ConfigValidationAnnotations.isString;

public class Flix2Config extends Config {
	
	@isString
    public static final String FLIX2_JDBC_MYSQL_FLIX2DB_HOST = "flix2.jdbc.mysql.flix2db.host";
	
	@isString
    public static final String FLIX2_JDBC_MYSQL_FLIX2DB_DBNAME = "flix2.jdbc.mysql.flix2db.dbname";
	
	@isString
    public static final String FLIX2_JDBC_MYSQL_FLIX2DB_USERNAME = "flix2.jdbc.mysql.flix2db.username";

	@isString
    public static final String FLIX2_JDBC_MYSQL_FLIX2DB_PASSWORD = "flix2.jdbc.mysql.flix2db.password";
	
	@isString
    public static final String FLIX2_TRACKING_DURATION_PARALLELISMHINT = "flix2.tracking.duration.parallelismHint";
	
	@isString
    public static final String FLIX2_TRACKING_TOPVIEW_PARALLELISMHINT = "flix2.tracking.topview.parallelismHint";
	
	@isString
    public static final String FLIX2_TRACKING_ERROR_LOG_PARALLELISMHINT = "flix2.tracking.errorlog.parallelismHint";

	@isString
    public static final String FLIX2_TRACKING_ERROR_LOG_CONNECTIONS_ERROR_COUNTS = "flix2.tracking.errorlog.connection.counts";

	@isString
    public static final String FLIX2_TRACKING_ERROR_LOG_TOKEN_ERROR_COUNTS = "flix2.tracking.errorlog.tokenerror.counts";
	
	@isString
    public static final String FLIX2_TRACKING_ERROR_LOG_SLACK_TOKEN= "flix2.tracking.errorlog.slack.token";
	
	@isString
    public static final String FLIX2_TRACKING_ERROR_LOG_SLACK_CHANNEL_LINKTV= "flix2.tracking.errorlog.slack.channel.linktv";
	
	@isString
    public static final String FLIX2_TRACKING_ERROR_LOG_SLACK_CHANNEL_LINKAPIS= "flix2.tracking.errorlog.slack.channel.linkapis";
	
	@isString
    public static final String FLIX2_TRACKING_ERROR_LOG_SLACK_CHANNEL_BACKENDAPI= "flix2.tracking.errorlog.slack.channel.backendapi";
	

}
