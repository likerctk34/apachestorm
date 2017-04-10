package flix2.stormkafka.mysql;

import com.zaxxer.hikari.HikariDataSource;

public class MySqlConnection {

	public static HikariDataSource CreateDataSource(String ip, String database, String username, String password) {
		HikariDataSource ds = new HikariDataSource();
		ds.setDriverClassName("com.mysql.jdbc.Driver");
		ds.setJdbcUrl("jdbc:mysql://" + ip + ":3306/" + database);
		ds.addDataSourceProperty("user", username);
		ds.addDataSourceProperty("password", password);
		ds.setMaximumPoolSize(20);
		ds.setConnectionTimeout(300000);
		return ds;
	}

}
