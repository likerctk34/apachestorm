package flix2.stormkafka.mysql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

import org.apache.log4j.Logger;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.tuple.Tuple;

public class MySqlTrackingView {

	public MySqlTrackingView(ConnectionProvider _connectionProvider) {
		this.connectionProvider = _connectionProvider;
	}

	public MySqlTrackingView() {
	}

	final static Logger logger = Logger.getLogger(MySqlTrackingView.class);
	ConnectionProvider connectionProvider;

	public void InsertOrUpdateToDB(Tuple tuple) throws Exception {
		Connection connRemote = null;
		try {
			String user_id = tuple.getStringByField("user_id");
			String profile_id = tuple.getStringByField("profile_id");
			String device_id = tuple.getStringByField("device_id");
			String content_id = tuple.getStringByField("content_id");
			String timeServerStr = tuple.getStringByField("timeServer");
			String parent_id = tuple.getStringByField("parent_id");
			if (parent_id == null || parent_id == "") {
				parent_id = content_id;
			}
			String platform = tuple.getStringByField("platform");
			String kind = tuple.getStringByField("kind");
			int duration = (int) Float.parseFloat(tuple.getStringByField("duration"));
			int total_time = 0;
			String total_time_str = tuple.getStringByField("total_time");
			if (total_time_str != "" && total_time_str != null) {
				total_time = (int) (Float.parseFloat(total_time_str) * 0.95);
			}
			//
			Date timeServer = new Date((long) Float.parseFloat(timeServerStr) * 1000L);
			Timestamp updated = new Timestamp(timeServer.getTime());
			if (duration >= total_time)
				duration = -1;

			// open connections
			connRemote = connectionProvider.getConnection();
			PreparedStatement statementUpdateRemote = null;
			PreparedStatement statementInsertRemote = null;
			// update flix_user_vod_viewed table
			statementUpdateRemote = connRemote.prepareStatement(QueryString.Update_flix_user_vod_viewed_query);
			statementUpdateRemote.setQueryTimeout(100);
			statementUpdateRemote.setInt(1, duration);
			statementUpdateRemote.setTimestamp(2, updated);
			statementUpdateRemote.setString(3, user_id);
			statementUpdateRemote.setString(4, profile_id);
			statementUpdateRemote.setString(5, device_id);
			statementUpdateRemote.setString(6, content_id);
			int result = statementUpdateRemote.executeUpdate();
			// if no rows updated, use insert
			if (result == 0) {
				statementInsertRemote = connRemote.prepareStatement(QueryString.Insert_flix_user_vod_viewed_query);
				statementInsertRemote.setQueryTimeout(100);
				statementInsertRemote.setString(1, user_id);
				statementInsertRemote.setString(2, profile_id);
				statementInsertRemote.setString(3, device_id);
				statementInsertRemote.setString(4, content_id);
				statementInsertRemote.setString(5, parent_id);
				statementInsertRemote.setString(6, kind);
				statementInsertRemote.setInt(7, duration);
				statementInsertRemote.setString(8, platform);
				statementInsertRemote.setTimestamp(9, updated);
				statementInsertRemote.executeUpdate();
				statementInsertRemote.close();
			}
			statementUpdateRemote.close();
		} catch (Exception e) {
			throw new Exception(e);
		} finally {
			closeConnection(connRemote);
		}
	}

	private void closeConnection(Connection connection) throws SQLException {
		if (connection != null) {
			connection.close();
		}
	}

}
