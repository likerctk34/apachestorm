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
package flix2.stormkafka.mysql;

public class QueryString {

	public static String Update_tracking_flix_watched_query = "UPDATE tracking_flix_watched SET "
			+ "duration=?,updated=?  WHERE user_id=? and profile_id=? " + "and device_id=? and content_id=?";

	public static String Insert_tracking_flix_watched_query = "INSERT into tracking_flix_watched "
			+ "(user_id, profile_id, device_id, content_id, parent_id,kind, duration, platform,updated) "
			+ "values (?, ?, ? , ?, ?, ?, ?, ?, ?)";

	public static String Update_flix_user_vod_viewed_query = "UPDATE flix_user_vod_viewed"
			+ " SET duration=?,updated=?  WHERE user_id=? and profile_id=? and device_id=? and content_id=?";

	public static String Insert_flix_user_vod_viewed_query = "INSERT into flix_user_vod_viewed "
			+ "(user_id, profile_id, device_id, content_id, parent_id,kind, duration, platform,updated)"
			+ " values (?, ?, ?,?,?,?,?,?,?)";
	
	//test
	
	public static String Insert_tracking_flix_watched_all_data_query = "INSERT into tracking_flix_watched "
			+ "(user_id, profile_id, device_id, content_id, parent_id,kind, duration, platform,updated , total_time) "
			+ "values (?, ?, ? , ?, ?, ?, ?, ?, ?,?)";
	
	public static String Insert_flix_user_vod_viewed_all_data_query = "INSERT into flix_user_vod_viewed_all_data "
			+ "(user_id, profile_id, device_id, content_id, parent_id,kind, duration, platform,updated,total_time)"
			+ " values (?, ?, ?,?,?,?,?,?,?,?)";
	
	public static String Insert_flix_user_vod_viewed_test_query = "INSERT into flix_user_vod_viewed_test "
			+ "(user_id, profile_id, device_id, content_id, parent_id,kind, duration, platform,updated)"
			+ " values (?, ?, ?,?,?,?,?,?,?)";
	
	public static String Update_flix_user_vod_viewed_test_query = "UPDATE flix_user_vod_viewed_test"
			+ " SET duration=?,updated=?  WHERE user_id=? and profile_id=? and device_id=? and content_id=?";
	
	public static String Insert_tracking_flix_watched_test_query = "INSERT into tracking_flix_watched_test "
			+ "(user_id, profile_id, device_id, content_id, parent_id,kind, duration, platform,updated) "
			+ "values (?, ?, ? , ?, ?, ?, ?, ?, ?)";
	
	public static String Update_tracking_flix_watched_test_query = "UPDATE tracking_flix_watched_test SET "
			+ "duration=?,updated=?  WHERE user_id=? and profile_id=? " + "and device_id=? and content_id=?";
	
	public static String Update_flix_top_view_query = "UPDATE flix_top_view"
			+ " SET count = count + 1 WHERE content_id =? and date_format(day,'%Y-%m-%d') = ?";

	public static String Insert_flix_top_view_query = "INSERT into flix_top_view "
			+ "(content_id,kind, count, day)"
			+ " values (?, ?, ?, ?)";
	
	public static String Update_flix_top_view_query_test = "UPDATE flix_top_view_test2"
			+ " SET count = count + 1 WHERE content_id =? and date_format(day,'%Y-%m-%d') = ?";

	public static String Insert_flix_top_view_query_test = "INSERT into flix_top_view_test"
			+ "(kind,content_id, day,count)"
			+ " values (?, ?, ?, ?)";

	public static String Insert_flix_vod_play_events_query = "insert into flix_play_events "
			+ "(kind, user_id, timeClient, parent_id, ip, profile_id,"
			+ " timeServer, user_agent, platform, action, content_id, device_id ) "
			+ "values (? ,? ,? ,? ,? ,? "
			+ " , ? ,? ,? ,? ,? , ?)";
	public static String Insert_flix_vod_time_spent_query = "insert into flix_time_spent_trident"
			+ "(kind,action, user_id,parent_id, profile_id,"
			+ " timeServer,timeClient,content_id, device_id ,total_time ,time_spent) "
			+ "values (? ,? ,? ,? ,? ,?, ?, ? "
			+ " ,? ,? ,?)";
	public static String Insert_flix_vod_time_spent_query_test = "insert into flix_time_spent_test2"
			+ "(kind,action, user_id,parent_id, profile_id,"
			+ " timeServer, content_id, device_id ,total_time ,time_spent) "
			+ "values (? ,? ,? ,? ,? ,STR_TO_DATE(?,'%Y-%m-%d %H:%i:%s'), ? "
			+ " ,? ,? ,?)";
	
	public static String pro_update_top_view_query = "call pro_update_top_view(? ,? ,?);";
	public static String pro_update_top_view_trident_query = "call pro_update_top_view_trident(? ,? ,? , ?);";
	public static String pro_update_flix_user_vod_viewed_query = "call pro_update_flix_user_vod_viewed(?,?,?,?,?,?,?,?,?);";
	
	public static String get_count_by_contentid_and_day_query = "select count from flix_top_view_trident_test "
			+ "where day = ? and content_id = ?";
	


}
