package flix2.stormkafka.mysql;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;

import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.tuple.Tuple;

public class MySqlPlayEvent {

	final static Logger logger = Logger.getLogger(MySqlPlayEvent.class);

	public MySqlPlayEvent(ConnectionProvider _connectionProvider) {
		this.connectionProvider = _connectionProvider;
	}

	public MySqlPlayEvent() {
	}

	ConnectionProvider connectionProvider;

	public int InsertOrUpdateToDB(Tuple tuple) throws SQLException {
		String kind = tuple.getStringByField("kind");
		String user_id = tuple.getStringByField("user_id");

		// get time Client
		String timeClientStr = tuple.getStringByField("timeClient");
		Date dateClient = new Date((long) Float.parseFloat(timeClientStr));
		Timestamp timeClient = new Timestamp(dateClient.getTime());

		String parent_id = tuple.getStringByField("parent_id");
		String ip = tuple.getStringByField("ip");
		String profile_id = tuple.getStringByField("profile_id");
		// get time Server
		String dateServerStr = tuple.getStringByField("timeServer");
		Date dateServer = new Date((long) Float.parseFloat(dateServerStr) * 1000L);
		dateServerStr = new SimpleDateFormat("yyyy-MM-dd").format(dateServer);
		Timestamp timeServer = new Timestamp(dateServer.getTime());

		String user_agent = tuple.getStringByField("user_agent");
		String platform = tuple.getStringByField("platform");
		String action = tuple.getStringByField("action");
		String content_id = tuple.getStringByField("content_id");
		String device_id = tuple.getStringByField("device_id");

		Connection connRemote = null;
		try {

			// open connections
			connRemote = connectionProvider.getConnection();
			PreparedStatement statementInsertViewRemote = null;
			if (action != null && action != "" && action.equals("play")) {
				statementInsertViewRemote = connRemote.prepareStatement(QueryString.Insert_flix_vod_play_events_query);
				// set paramaters
				statementInsertViewRemote.setString(1, kind);
				statementInsertViewRemote.setString(2, user_id);
				statementInsertViewRemote.setTimestamp(3, timeClient);
				statementInsertViewRemote.setString(4, parent_id);
				statementInsertViewRemote.setString(5, ip);
				statementInsertViewRemote.setString(6, profile_id);
				statementInsertViewRemote.setTimestamp(7, timeServer);
				statementInsertViewRemote.setString(8, user_agent);
				statementInsertViewRemote.setString(9, platform);
				statementInsertViewRemote.setString(10, action);
				statementInsertViewRemote.setString(11, content_id);
				statementInsertViewRemote.setString(12, device_id);
				statementInsertViewRemote.executeUpdate();
			}

			// close statment
			if (statementInsertViewRemote != null) {
				try {
					statementInsertViewRemote.close();
				} catch (Exception ex) {
					logger.error(ex.getMessage());
				}
			}
			return 1;
		} catch (Exception e) {
			logger.error(e.getMessage());
			System.out.println(e.getMessage());
			return -1;
		}

		finally {
			closeConnection(connRemote);
		}
	}

	private void closeConnection(Connection connection) {
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				throw new RuntimeException("Failed to close connection", e);
			}
		}
	}
}
