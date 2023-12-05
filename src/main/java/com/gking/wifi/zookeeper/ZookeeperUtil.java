package com.gking.wifi.zookeeper;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
* @author gking
* @date  2016年9月9日 
* 
*/
public class ZookeeperUtil {
	/**
	 * 
	 * @author liliwei
	 * @date  2016年9月9日 
	 * 
	 * 批量更新zookeeper值
	 * @param hostname ip+port ,zookeeper default port : 2181 ,
	 * 		  e.g.  10.1.1.1:2181
	 * @param keyValues   key must with absolute path and start with '/'
	 * @throws Exception
	 */
	public void updatePaths(String hostname,Map<String,String> keyValues) throws Exception {
		CuratorFramework client = CuratorFrameworkFactory.builder()
				.connectString(hostname)
				.sessionTimeoutMs(5000)
				.connectionTimeoutMs(3000)
				.retryPolicy(new ExponentialBackoffRetry(1000, 3))
				.build();
		client.start();
		for (Map.Entry<String, String> entry : keyValues.entrySet()) {
			client.setData().forPath(entry.getKey(), entry.getValue().getBytes());
		}
	

		client.close();
	}
	
	
	/**
	 * @author liliwei
	 * @date  2016年9月9日 
	 * 
	 * @param hostname ip+port ,zookeeper default port : 2181 ,
	 * 		  e.g.  10.1.1.1:2181
	 * @param key 数据key key must with absolute path and start with '/'
	 * @param value 数据value
	 */
	public boolean updateData(String hostname,String key,String value)  {
		String path = key;
		CuratorFramework client = CuratorFrameworkFactory.builder()
				.connectString(hostname)
				.sessionTimeoutMs(5000)
				.connectionTimeoutMs(3000)
				.retryPolicy(new ExponentialBackoffRetry(1000, 3))
				.build();
		client.start();
		try {
			if(client.checkExists().forPath(path)==null){
				client.create().forPath(path, value.getBytes());
			}
			client.setData().forPath(path, value.getBytes());
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		client.close();
		return true;
	}
	
	
	/**
	 * @author liliwei
	 * @date  2016年9月9日 
	 * 
	 * @param hostname ip+port ,zookeeper default port : 2181 ,
	 * 		  e.g.  10.1.1.1:2181
	 * @param type 
	 * 		  e.g.   cpa  pdb etc.
	 * @param key 数据key 
	 * @param value 数据value
	 */
	public boolean createOrupdateData(String hostname, String type,String key,String value)  {
		String path = "/yisaconfig/"+type+"/"+key;
		CuratorFramework client = CuratorFrameworkFactory.builder()
				.connectString(hostname)
				.sessionTimeoutMs(5000)
				.connectionTimeoutMs(1000)
				.retryPolicy(new ExponentialBackoffRetry(1000, 3))
				.build();
		client.start();
		try {
			if(client.checkExists().forPath(path)==null){
				if(client.checkExists().forPath("/yisaconfig/"+type)==null){
					client.create().forPath("/yisaconfig/"+type, type.getBytes());
				}
				client.create().forPath(path, value.getBytes());
			}
			client.setData().forPath(path, value.getBytes());
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		client.close();
		return true;
	}
	
	/**
	 * @author liliwei
	 * @date  2016年9月9日 
	 * 
	 * @param hostname ip+port ,zookeeper default port : 2181 ,
	 * 		  e.g.  10.1.1.1:2181
	 * @param key must with absolute path and start with '/'
	 * @return
	 */
	public String getData(String hostname,String key)  {
		CuratorFramework client = CuratorFrameworkFactory.builder()
				.connectString(hostname)
				.sessionTimeoutMs(5000)
				.connectionTimeoutMs(3000)
				.retryPolicy(new ExponentialBackoffRetry(1000, 3))
				.build();
		client.start();
		String data="";
		try {
			data= new String(client.getData().forPath(key), "utf8");
		} catch (Exception e) {
			e.printStackTrace();
		}
		client.close();
		return data;
	}
	
	/**
	 * @author liliwei
	 * @date  2016年9月21日 
	 * @param hostname ip+port ,zookeeper default port : 2181 ,
	 * 		  e.g.  10.1.1.1:2181
	 * @param type  
	 * 		  e.g.   cpa  pdb etc.
	 * @param withCommon  结果是否包含common
	 * @return Map<String,String> with all config be defined in type 
	 * @throws Exception 
	 * @throws UnsupportedEncodingException 
	 */
	public 	Map<String,String> getAllConfig(String hostname,String type,boolean withCommon) throws UnsupportedEncodingException, Exception  {
		
		String rootPath = "/yisaconfig/"+type;
		
		CuratorFramework client = CuratorFrameworkFactory.builder()
				.connectString(hostname)
				.sessionTimeoutMs(5000)
				.connectionTimeoutMs(3000)
				.retryPolicy(new ExponentialBackoffRetry(1000, 3))
				.build();
		client.start();
	
		Map<String, String> configs = new HashMap<String, String>();
		List<String> children = client.getChildren().forPath(rootPath);
		for (String path : children) {
			String data = new String(client.getData().forPath(rootPath + "/" + path), "utf8");
			configs.put(path, data);
		}
		if (withCommon) {
			String commonPath = "/yisaconfig/common";
			List<String> childrenCommon = client.getChildren().forPath(commonPath);
			for (String path : childrenCommon) {
				String data = new String(client.getData().forPath(commonPath + "/" + path), "utf8");
				configs.put(path, data);
			}
		}

		client.close();
		return configs;
	}

	/**
	 * @author liliwei
	 * @date  2016年9月21日 
	 * @param hostname ip+port ,zookeeper default port : 2181 ,
	 * 		  e.g.  10.1.1.1:2181
	 * @param type 
	 * 		  e.g.   cpa  pdb etc.
	 * @param key
	 * 		  key in type ,e.g. hdfs kafkaip etc.
	 * @return
	 * @throws Exception 
	 * @throws UnsupportedEncodingException 
	 */
	public 	String getConfig(String hostname,String type,String key) throws UnsupportedEncodingException, Exception  {
		
		String path = "/yisaconfig/"+type+"/"+key;
		
		CuratorFramework client = CuratorFrameworkFactory.builder()
				.connectString(hostname)
				.sessionTimeoutMs(5000)
				.connectionTimeoutMs(3000)
				.retryPolicy(new ExponentialBackoffRetry(1000, 3))
				.build();
		client.start();
		String data = "";
		data = new String(client.getData().forPath(path), "utf8");

		client.close();
		return data;
	}
	
	private 	Map<String,String> getCommonConfig(String hostname,String type,boolean withCommon)  {
		
		String rootPath = "/yisaconfig/"+type;
		
		CuratorFramework client = CuratorFrameworkFactory.builder()
				.connectString(hostname)
				.sessionTimeoutMs(5000)
				.connectionTimeoutMs(3000)
				.retryPolicy(new ExponentialBackoffRetry(1000, 3))
				.build();
		client.start();
	
		Map<String,String> configs = new HashMap<String, String>();
		try {
			List<String> children = client.getChildren().forPath(rootPath);
			for (String path : children) {
				String data=new String(client.getData().forPath(rootPath+"/"+path), "utf8");
				configs.put(path, data);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		client.close();
		return configs;
	}
}
