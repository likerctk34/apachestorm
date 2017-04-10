package org.apache.storm.jdbc.bolt;

import java.sql.SQLException;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import flix2.stormkafka.mysql.MySqlTopView;
import flix2.stormkafka.mysql.MySqlTrackingView;
import flix2.stormkafka.scheme.TrackingViewScheme;

 
public class TopViewEventInsertBolt extends BaseRichBolt{
 
 final static Logger logger = Logger.getLogger(TopViewEventInsertBolt.class);
 private static final long serialVersionUID = 1L;
 private MySqlTopView mySqlInsertUpdate;
 private ConnectionProvider connectionProvider;
 OutputCollector _collector;
 
 public TopViewEventInsertBolt(){
	 
 }
 
public TopViewEventInsertBolt(ConnectionProvider _connectionProvider){
	 this.connectionProvider = _connectionProvider;
 }
 @Override
 public void prepare(Map conf, TopologyContext context, OutputCollector collector)
 {
	 this._collector = collector;
	 connectionProvider.prepare();
	 mySqlInsertUpdate = new MySqlTopView(connectionProvider);
 }
  
 @Override
 public void execute(Tuple tuple)  {
    String kind = tuple.getStringByField("kind");
	boolean isUpdateTracking = (kind.equals("v") || kind.equals("c"));
	if(isUpdateTracking){
			try {
				mySqlInsertUpdate.InsertOrUpdateToDB(tuple);
				this._collector.ack(tuple);
			} catch (Exception e) {
				this._collector.fail(tuple);
			}
			//may be send message here
		}
 }
 
 public void declareOutputFields(OutputFieldsDeclarer declarer) {
  // TODO Auto-generated method stub
   
 }
  
 @Override
    public void cleanup() {
	 
	 connectionProvider.cleanup();
 }

 
}
