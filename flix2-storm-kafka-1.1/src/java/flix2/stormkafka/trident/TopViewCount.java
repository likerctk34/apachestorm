package flix2.stormkafka.trident;

import java.sql.Date;

import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;

import flix2.stormkafka.mysql.MySqlUtil;

public class TopViewCount implements CombinerAggregator<Integer> {

	private ConnectionProvider _connectionProvider;

	public TopViewCount(ConnectionProvider connectionProvider) {
		this._connectionProvider = connectionProvider;
	}
	
	public TopViewCount() {
	}

	@Override
	public Integer init(TridentTuple tuple) {
		/*int size = tuple.getValues().size();
		if (size > 0) {
			String content_id = (String) tuple.getValue(1);
			String dateServerStr = (String) tuple.getValue(2);
			return MySqlUtil.getCountContentIdByDay(this._connectionProvider, content_id, dateServerStr);
		}*/
		return 1;
	}

	@Override
	public Integer combine(Integer val1, Integer val2) {
		return val1 + val2;
	}

	@Override
	public Integer zero() {
		return 0;
	}

}
