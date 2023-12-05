package com.gking.wifi.zookeeper;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

import com.gking.wifi.common.RemoteShell;
import com.gking.wifi.common.SQLUtil;

/**
 * @author gking
 * @date
 * 
 */
public class ZKListener {

	public static void main(String[] args) throws Exception {

		/*
		 * get jar parameter parameter form: listenTo=192.168.0.100:2181
		 * getConfFrom=192.168.1.20:3306/om_center,root,yisa_omnieye
		 */
		HashMap<String, String> conf = new HashMap<String, String>();
		String zkAddr = "";
		String dbAddr = "";
		String dbUser = "";
		String dbPassword = "";
		for (int i = 0; i < args.length; i++) {
			String[] temp1 = args[i].split("=");
			conf.put(temp1[0], temp1[1]);
		}

		Set<String> confKeySet = conf.keySet();
		if (confKeySet.size() != 2) {
			System.out
					.println("Wrong number of parameters: Two parameters are needed");
			System.exit(1);
		} else if (!confKeySet.contains("listenTo")) {
			System.out.println("Need parameter: listenTo");
			System.exit(1);
		} else if (!confKeySet.contains("getConfFrom")) {
			System.out.println("Need parameter: getConfFrom");
			System.exit(1);
		} else {
			zkAddr = conf.get("listenTo");
			String[] temp2 = conf.get("getConfFrom").split(",");
			dbAddr = temp2[0];
			dbUser = temp2[1];
			dbPassword = temp2[2];
		}
		System.out.println("listenTo=" + zkAddr + " getConfFrom=" + dbAddr
				+ ",user:" + dbUser + ",password:" + dbPassword);

		CuratorFramework client = CuratorFrameworkFactory
				.builder()
				// .connectString("192.168.0.157:2181")
				.connectString(zkAddr).sessionTimeoutMs(500000)
				.connectionTimeoutMs(300000)
				.retryPolicy(new ExponentialBackoffRetry(1000, 3)).build();
		client.start();
		System.out.println("Connected to the ZK!");
		/**
		 * 在注册监听器的时候，如果传入此参数，当事件触发时，逻辑由线程池处理
		 */
		// ExecutorService pool = Executors.newFixedThreadPool(2);

		/**
		 * 监听数据节点的变化情况
		 */
		final NodeCache nodeCache = new NodeCache(client, "/updateconfig",
				false);
		final String sub_dbAddr = dbAddr;
		final String sub_dbUser = dbUser;
		final String sub_dbPassword = dbPassword;
		final SQLUtil sqlUtil = new SQLUtil();
		nodeCache.start(true);
		System.out.println("listening.....");

		try {

			/*
			 * 逻辑由线程池处理 nodeCache.getListenable().addListener( new
			 * NodeCacheListener() {
			 * 
			 * @Override public void nodeChanged() throws Exception {
			 * System.out.println("Node data is changed, new data: " + new
			 * String(nodeCache.getCurrentData().getData())); } }, pool );
			 */

			nodeCache.getListenable().addListener(new NodeCacheListener() {
				@Override
				public void nodeChanged() throws Exception {
					System.out.println("ZKNode data has been changed, new data: "
							+ new String(nodeCache.getCurrentData().getData()));
					
					// connect to the omDB of om_center
					Connection dbconn = sqlUtil.connectDB(sub_dbAddr,
							sub_dbUser, sub_dbPassword);
					System.out.println("Login the omDB succeed!");
					// Get the corresponding relationship between node_id and node_ip
					HashMap<String, String> nodeid_nodeip = null;
					ArrayList<HashMap<String, String>> rs_list = null;
					String query = null;

					nodeid_nodeip = new HashMap<String, String>();
					query = "SELECT NODE_ID as NODE_ID , hostport as HOSTPORT FROM zk_node";
					rs_list = sqlUtil.executeSql(query, dbconn);
					for (HashMap<String, String> rs : rs_list) {
						System.out.println("zk_node table ---- " + rs.get("NODE_ID") + ":" + rs.get("HOSTPORT") + " ----");
						nodeid_nodeip.put(rs.get("NODE_ID"), rs.get("HOSTPORT"));
					}
					// end

					// The value of the node is in the form of
					// "297,384,5643,3754", so we need to split them by ","
					// and go through every id, update the config by the id.
					String[] newZKvalueList = new String(nodeCache.getCurrentData().getData()).split(",");
					String node_id = null, module = null, key_name, value_name, is_restart;
					boolean needRestart = false;

					for (int i = 0; i < newZKvalueList.length; i++) {
						rs_list = new ArrayList<HashMap<String, String>>();
						String theNewValue = newZKvalueList[i];
						query = "SELECT t1.node_id as NODE_ID ,t2.module as MODULE,t2.key_name as KEY_NAME,t3.value_name as VALUE_NAME,t2.is_restart as IS_RESTART "
								+ "FROM sys_properties_nodeid t1 "
								+ "INNER JOIN sys_properties_value t3 "
								+ "ON t1.templet_id = t3.value_templet_id "
								+ "INNER JOIN sys_properties_key t2 "
								+ "ON t2.key_id = t3.value_key_id "
								+ "WHERE ID = " + theNewValue;
//						System.out.println(query);

						// execute the query.
						rs_list = sqlUtil.executeSql(query, dbconn);
						HashMap<String, String> rs = null;
						System.out.println(rs_list.size());
						if (rs_list.size() != 1) {
							System.out.println("Error:Database query results are not unique! Please check the query:\n" + query);
						} else {
							rs = rs_list.get(0);
						}

						// update the config
						node_id = rs.get("NODE_ID");
						module = rs.get("MODULE");
						key_name = rs.get("KEY_NAME");
						value_name = rs.get("VALUE_NAME");
						is_restart = rs.get("IS_RESTART");

						if (!needRestart && is_restart.equals("1")) {
							needRestart = true;
						}
						
//						System.out.println(MessageFormat.format("node_id:{0}\nmodule:{1}\nkey_name:{2}\nvalue_name:{3}", 
//								node_id, module, key_name, value_name));

						try{
							ZookeeperUtil zkut = new ZookeeperUtil();
							zkut.createOrupdateData(nodeid_nodeip.get(node_id),
									module, key_name, value_name);
						}catch (Exception e) {
							// TODO Auto-generated catch block
							needRestart = false;
							e.printStackTrace();
						}
						
						
					}
					// end

					// execute the restart script
					if (needRestart) {
						HashMap<String, String> remoteshell_rs = null;
						ArrayList<HashMap<String, String>> remoteshell_list = null;
						String remoteshell_query;
						remoteshell_query = "SELECT hostport as HOSTPORT , path as PATH, username as USERNAME, password as PASSWORD FROM "
								+ "zk_remoteshell WHERE module = '"
								+ module
								+ "' AND node_id = '" + node_id + "'";
//						System.out.println(remoteshell_query);
						remoteshell_list = sqlUtil.executeSql(remoteshell_query, dbconn);

						if (remoteshell_list.size() != 1) {
							System.out.println("Error:Database query results are not unique! Please check the remoteshell_query:\n"
											+ remoteshell_query);
						} else {
							remoteshell_rs = remoteshell_list.get(0);
						}
						RemoteShell shell = new RemoteShell(remoteshell_rs.get("HOSTPORT"), 
								remoteshell_rs.get("USERNAME"), remoteshell_rs.get("PASSWORD"));
						String shellOutPut = shell.exec("python " + remoteshell_rs.get("PATH"));
						System.out.println("####Restarting server: ####\n" + shellOutPut);
					}
					// end

					System.out.println("Config changing succeed!!");
				}
			});
			// client.setData().forPath("-huey/cnode", "world".getBytes());

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Thread.sleep(Long.MAX_VALUE);
		// pool.shutdown();
		nodeCache.close();
		client.close();

	}

}
