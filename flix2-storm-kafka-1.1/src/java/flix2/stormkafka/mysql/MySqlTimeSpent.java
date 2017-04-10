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

public class MySqlTimeSpent {

	final static Logger logger = Logger.getLogger(MySqlTimeSpent.class);

	public MySqlTimeSpent(ConnectionProvider _connectionProvider) {
		this.connectionProvider = _connectionProvider;
	}

	public MySqlTimeSpent() {
	}

	ConnectionProvider connectionProvider;

	public int InsertOrUpdateToDB(Tuple tuple) throws SQLException {
		// get time Server
		String dateServerStr = tuple.getStringByField("timeServer");
		Date dateServer = new Date((long) Float.parseFloat(dateServerStr) * 1000L);
		dateServerStr = new SimpleDateFormat("yyyy-MM-dd").format(dateServer);
		Timestamp timeServer = new Timestamp(dateServer.getTime());
		String kind = tuple.getStringByField("kind");
		String user_id = tuple.getStringByField("user_id");
		String parent_id = tuple.getStringByField("parent_id");
		String profile_id = tuple.getStringByField("profile_id");
		String content_id = tuple.getStringByField("content_id");
		String device_id = tuple.getStringByField("device_id");
		String total_time = tuple.getStringByField("total_time");
		String time_spent = tuple.getStringByField("time_spent");

		Connection connRemote = null;
		try {

			// open connections
			connRemote = connectionProvider.getConnection();
			PreparedStatement statementInsertViewRemote = null;
			statementInsertViewRemote = connRemote.prepareStatement(QueryString.Insert_flix_vod_time_spent_query);
			// set paramaters
			statementInsertViewRemote.setString(1, kind);
			statementInsertViewRemote.setString(2, user_id);
			statementInsertViewRemote.setString(3, parent_id);
			statementInsertViewRemote.setString(4, profile_id);
			statementInsertViewRemote.setTimestamp(5, timeServer);
			statementInsertViewRemote.setString(6, content_id);
			statementInsertViewRemote.setString(7, device_id);
			statementInsertViewRemote.setString(8, total_time);
			statementInsertViewRemote.setString(9, time_spent);
			statementInsertViewRemote.executeUpdate();

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
