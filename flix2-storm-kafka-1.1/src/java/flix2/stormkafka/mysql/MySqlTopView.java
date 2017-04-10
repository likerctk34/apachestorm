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

public class MySqlTopView {

	final static Logger logger = Logger.getLogger(MySqlTopView.class);

	public MySqlTopView(ConnectionProvider _connectionProvider) {
		this.connectionProvider = _connectionProvider;
	}

	public MySqlTopView() {
	}

	ConnectionProvider connectionProvider;

	public int InsertOrUpdateToDB(Tuple tuple) throws SQLException {
		String kind = tuple.getStringByField("kind");
		String parent_id = tuple.getStringByField("parent_id");
		String dateServerStr = tuple.getStringByField("timeServer");
		Date dateServer = new Date((long) Float.parseFloat(dateServerStr) * 1000L);
		dateServerStr = new SimpleDateFormat("yyyy-MM-dd").format(dateServer);
		String content_id = tuple.getStringByField("content_id");

		Connection connRemote = null;
		try {

			// open connections
			connRemote = connectionProvider.getConnection();
			PreparedStatement statementUpdateTopViewRemote = null;
			PreparedStatement statementInsertTopViewRemote = null;
			if (parent_id != null && parent_id != "") {
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
				statementInsertTopViewRemote = connRemote.prepareStatement(QueryString.Insert_flix_top_view_query_test);
				statementInsertTopViewRemote.setQueryTimeout(100);
				statementInsertTopViewRemote.setString(1, content_id);
				statementInsertTopViewRemote.setString(2, kind);
				statementInsertTopViewRemote.setInt(3, 1);
				statementInsertTopViewRemote.setString(4, dateServerStr);
				statementInsertTopViewRemote.executeUpdate();
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
