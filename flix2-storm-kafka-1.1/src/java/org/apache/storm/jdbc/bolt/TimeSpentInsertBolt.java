/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.jdbc.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException;

import java.sql.BatchUpdateException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Basic bolt for writing to any Database table.
 * <p/>
 * Note: Each JdbcInsertBolt defined in a topology is tied to a specific table.
 */
public class TimeSpentInsertBolt extends AbstractJdbcBolt {
	private static final Logger LOG = LoggerFactory.getLogger(TimeSpentInsertBolt.class);

	private String tableName;
	private String insertQuery;
	private JdbcMapper jdbcMapper;

	public TimeSpentInsertBolt(ConnectionProvider connectionProvider, JdbcMapper jdbcMapper) {
		super(connectionProvider);

		Validate.notNull(jdbcMapper);
		this.jdbcMapper = jdbcMapper;
	}

	public TimeSpentInsertBolt withTableName(String tableName) {
		if (insertQuery != null) {
			throw new IllegalArgumentException("You can not specify both insertQuery and tableName.");
		}
		this.tableName = tableName;
		return this;
	}

	public TimeSpentInsertBolt withInsertQuery(String insertQuery) {
		if (this.tableName != null) {
			throw new IllegalArgumentException("You can not specify both insertQuery and tableName.");
		}
		this.insertQuery = insertQuery;
		return this;
	}

	public TimeSpentInsertBolt withQueryTimeoutSecs(int queryTimeoutSecs) {
		this.queryTimeoutSecs = queryTimeoutSecs;
		return this;
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
		super.prepare(map, topologyContext, collector);
		if (StringUtils.isBlank(tableName) && StringUtils.isBlank(insertQuery)) {
			throw new IllegalArgumentException("You must supply either a tableName or an insert Query.");
		}
	}

	@Override
	public void execute(Tuple tuple) {
		try {
			List<Column> columns = jdbcMapper.getColumns(tuple);
			List<List<Column>> columnLists = new ArrayList<List<Column>>();
			columnLists.add(columns);
			if (!StringUtils.isBlank(tableName)) {
				this.jdbcClient.insert(this.tableName, columnLists);
			} else {
				this.jdbcClient.executeInsertQuery(this.insertQuery, columnLists);
			}
			this.collector.ack(tuple);
			LOG.debug("insert tuple to mysql, tuple info : "  + tuple.toString());
		} catch (Exception e) {
			Throwable cause = e.getCause();
			if (cause instanceof BatchUpdateException) {
				Throwable cause2 = cause.getCause(); 
				if (cause2 instanceof MySQLIntegrityConstraintViolationException) {
					LOG.info("ignore fail excute insert duplicated rows");
					this.collector.ack(tuple);
				}
			} else {
				LOG.error(e.getMessage());
				this.collector.reportError(e);
				this.collector.fail(tuple);
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

	}
}
