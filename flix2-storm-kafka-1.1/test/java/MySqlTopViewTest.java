/*

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import org.apache.log4j.Logger;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.tuple.Tuple;

import flix2.stormkafka.mysql.QueryString;

public class MySqlTopViewTest {

	public MySqlTopViewTest(ConnectionProvider _connectionProvider) {
		this.connectionProvider = _connectionProvider;
	}

	public MySqlTopViewTest() {
	}

	final static Logger logger = Logger.getLogger(MySqlTopViewTest.class);
	ConnectionProvider connectionProvider;

	public int InsertOrUpdateToDB(Tuple tuple) throws SQLException {
		String kind = tuple.getStringByField("kind");
		String user_id = tuple.getStringByField("user_id");

		// get time Client
		String timeClientStr = tuple.getStringByField("timeClient");
		Date dateClient = new Date((long) Float.parseFloat(timeClientStr) * 1000L);
		Timestamp timeClient = new Timestamp(dateClient.getTime());
		//
		// Date timeServer = new Date((long) Float.parseFloat(timeServerStr) *
		// 1000L);
		// Timestamp updated = new Timestamp(timeServer.getTime());
		//

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
			if(action != null && action != "" && action.equals("play")){
				statementInsertViewRemote = connRemote.prepareStatement(QueryString.Insert_flix_vod_play_events_query);
				// set paramaters
				statementInsertViewRemote.setString(1, kind);
				statementInsertViewRemote.setString(2, user_id);
				// work around change to time server becaues can not parse time
				// client to date time
				statementInsertViewRemote.setTimestamp(3, timeServer);
				statementInsertViewRemote.setString(4, parent_id);
				statementInsertViewRemote.setString(5, ip);
				statementInsertViewRemote.setString(6, profile_id);
				statementInsertViewRemote.setTimestamp(7, timeServer);
				statementInsertViewRemote.setString(8, user_agent);
				statementInsertViewRemote.setString(9, platform);
				statementInsertViewRemote.setString(10, action);
				statementInsertViewRemote.setString(11, content_id);
				statementInsertViewRemote.setString(12, device_id);
				//
				statementInsertViewRemote.executeUpdate();
			}
			
			
			PreparedStatement statementUpdateTopViewRemote = null;
			PreparedStatement statementInsertTopViewRemote = null;
			if ( !kind.equals("i") && action != null && action != "" && action.equals("play")) {
				if(parent_id != null && parent_id != "" ){
					content_id = parent_id;
					if (kind != null && kind != "") {
						if (kind.equals("v")) {
							kind = "e";
						}

						if (kind.equals("c")) {
							kind = "t";
						}

					}
				}

				// prepare statments
				// update flix_user_vod_viewed table
				statementUpdateTopViewRemote = connRemote.prepareStatement(QueryString.Update_flix_top_view_query_test);
				statementUpdateTopViewRemote.setQueryTimeout(100);
				statementUpdateTopViewRemote.setString(1, content_id);
				statementUpdateTopViewRemote.setString(2, dateServerStr);
				int result = statementUpdateTopViewRemote.executeUpdate();
				// if no rows updated, use insert
				if (result == 0) {
					statementInsertTopViewRemote = connRemote
							.prepareStatement(QueryString.Insert_flix_top_view_query_test);
					statementInsertTopViewRemote.setQueryTimeout(100);
					statementInsertTopViewRemote.setString(1, content_id);
					statementInsertTopViewRemote.setString(2, kind);
					statementInsertTopViewRemote.setInt(3, 1);
					statementInsertTopViewRemote.setString(4, dateServerStr);
					statementInsertTopViewRemote.executeUpdate();
				}
			}

			// close statment
			if (statementInsertViewRemote != null) {
				try {
					statementInsertViewRemote.close();
				} catch (Exception ex) {
					logger.error(ex.getMessage());
				}
			}

			// close statment
			if (statementUpdateTopViewRemote != null) {
				try {
					statementUpdateTopViewRemote.close();
				} catch (Exception ex) {
					logger.error(ex.getMessage());
				}
			}

			// close statment
			if (statementInsertTopViewRemote != null) {
				try {
					statementInsertTopViewRemote.close();
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

		//
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
*/