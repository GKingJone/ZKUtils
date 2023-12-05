package com.gking.wifi.common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

/**
* @author gking
* @date  2016年9月9日 
* 
*/
public class SQLUtil {

	private Connection conn = null;

	public void closeConnect(Connection conn) {
		try {
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public Connection connectDB(String dbAddr, String dbUser, String dbPassword) {

		//connect mysql
		try {
			System.out.println("--------------------------");
			if (conn == null || conn.isClosed()) {
				Class.forName("com.mysql.jdbc.Driver");
				//String url = "jdbc:mysql://192.168.0.157:3306/"
				String url = "jdbc:mysql://" + dbAddr
						+ "?useUnicode=true&characterEncoding=UTF8";
//				conn = DriverManager.getConnection(url);
				conn = DriverManager.getConnection(url, dbUser, dbPassword);
				System.out.println("Mysql connected!");

			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return conn;
	}

	public ArrayList<HashMap<String, String>> executeSql(String query, Connection conn) throws Exception {

		//execute sql statement
		ArrayList<HashMap<String, String>> result_list = new ArrayList<HashMap<String, String>>();
		ResultSet rs = null;
		Statement statement = null;
		try {
			statement = conn.createStatement();
			rs = statement.executeQuery(query);
			while (rs.next()) {
				ResultSetMetaData rsmd = rs.getMetaData();
				HashMap<String, String> result = new HashMap<String, String>();
				for (int i = 1; i <= rsmd.getColumnCount(); i++) {
					result.put(rsmd.getColumnName(i), rs.getString(rsmd.getColumnName(i)));
				}
				result_list.add(result);
			}
		} finally {
			statement.close();
		}
		System.out.println("sql executed!");
		return result_list;
	}

	/*
	public HashMap<String, String> getConfig(String dbAddr) throws Exception{
		//connect mysql
		Connection conn = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			//String url = "jdbc:mysql://192.168.0.157:3306/"
			String url = "jdbc:mysql://"
					+ dbAddr
			        + "?user=root&password=yisa_omnieye&useUnicode=true&characterEncoding=UTF8";
			conn = DriverManager.getConnection(url);
			System.out.println("Mysql connected!");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("Mysql connecting failed!");
			e.printStackTrace();
		}
		
		//execute sql statement
		String sql = "SELECT * from config;";
		HashMap<String, String> result = new HashMap<String, String>();
		ResultSet rs = null;
		try {
			Statement statement = conn.createStatement();
			rs = statement.executeQuery(sql);
			while(rs.next()){
	
				String key=rs.getString("key");
				String value=rs.getString("value");
				result.put(key, value);
			}
			
			System.out.println("sql executed!");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		conn.close();
		return result;
	}
	*/

}
