package flix2.stormkafka.mysql;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.storm.jdbc.common.ConnectionProvider;

import com.zaxxer.hikari.HikariDataSource;

public class HikariDataSourceProvider implements ConnectionProvider {

	public HikariDataSourceProvider(String ip, String database, String username, String password) {
		this.setIp(ip);
		this.setDatabase(database);
		this.setUsername(username);
		this.setPassword(password);
	}

	String ip;

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	private String database;
	private String username;
	private String password;
	private HikariDataSource dsRemote;

	public HikariDataSource getDsRemote() {
		return dsRemote;
	}

	public void setDsRemote(HikariDataSource dsRemote) {
		this.dsRemote = dsRemote;
	}

	@Override
	public void prepare() {
		if (dsRemote == null) {
			dsRemote = MySqlConnection.CreateDataSource(this.getIp(), this.getDatabase(), this.getUsername(),
					this.getPassword());
		}
	}

	@Override
	public void cleanup() {

		if (dsRemote != null) {
			dsRemote.close();
		}
		// TODO Auto-generated method stub

	}

	@Override
	public Connection getConnection() {
		try {
			return dsRemote.getConnection();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

}
