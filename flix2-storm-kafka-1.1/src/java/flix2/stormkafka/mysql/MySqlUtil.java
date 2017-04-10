package flix2.stormkafka.mysql;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.apache.storm.jdbc.common.ConnectionProvider;

public class MySqlUtil {

	final static Logger logger = Logger.getLogger(MySqlUtil.class);

	public static int getCountContentIdByDay(ConnectionProvider connectionProvider, String content_id, String day) {
		Connection connRemote = null;
		int count = 0;
		try {
			// open connections
			connRemote = connectionProvider.getConnection();
			PreparedStatement statementSelectCountView = null;
			statementSelectCountView = connRemote
					.prepareStatement(QueryString.get_count_by_contentid_and_day_query);
			// set paramaters
			statementSelectCountView.setString(1, day);
			statementSelectCountView.setString(2, content_id);
			ResultSet  rs = statementSelectCountView.executeQuery();
			if(rs.next()){
				count =  rs.getInt("count");
			}

			// close statment
			if (statementSelectCountView != null) {
				try {
					statementSelectCountView.close();
				} catch (Exception ex) {
					logger.error(ex.getMessage());
				}
			}
			
		} catch (Exception e) {
			logger.error(e.getMessage());
		}

		finally {
			closeConnection(connRemote);
		}
		return count;

	}

	private static void closeConnection(Connection connection) {
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				throw new RuntimeException("Failed to close connection", e);
			}
		}
	}
}
