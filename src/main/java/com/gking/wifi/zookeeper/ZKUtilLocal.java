package com.gking.wifi.zookeeper;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.kohsuke.args4j.Option;

/**
* @author gking
* @date  2016年6月25日 
* 
*/
public class ZKUtilLocal implements Watcher {
	private static final int SESSION_TIMEOUT = 10000;
	private ZooKeeper zk = null;
	private CountDownLatch countDownLatch = new CountDownLatch(1);
	
	@Option(name = "--create", usage = "")
	private boolean create;
	
	@Option(name = "--set", usage = "")
	private boolean set;
	
	@Option(name = "--del", usage = "")
	private boolean del;
	
	@Option(name = "--get", usage = "")
	private boolean get;
	
	@Option(name = "--list", usage = "")
	private boolean list;
	
	
	@Option(name = "-value", usage = "")
	private String value = "default";
	
	@Option(name = "-path", usage = "")
	private String path = "/default";
	
	
	@Option(name = "-zk", usage = "")
	private String zkconn = "cdh1:2181";

	public static void main(String[] args) {
		
		
		ZKUtilLocal sample = new ZKUtilLocal();
		

		sample.doAction(args);
		sample.releaseConnection();
	}
	
	public void doAction(String[] args) {
		
		
		
		createConnection(zkconn, SESSION_TIMEOUT);
		
		deleteNodeRecursion("/kafka");
		deleteNode("/kafka");
		
		
		releaseConnection();
	}
	
	

	private void listData(String path) {
		try {
			if (zk.exists(path, null) != null) {
				readData(path);
				List<String> childs = zk.getChildren(path, null);
				if (childs != null && childs.size() > 0) {
					for (String path_c : childs) {
						listData(path+"/"+path_c);
					}
				}
			}
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
	}

	//连接zk集群
	public void createConnection(String connectString, int sessionTimeout) {
		try {
			zk = new ZooKeeper(connectString, sessionTimeout, this);
			countDownLatch.await(); 
		} catch (Exception e) {
			System.out.println("连接创建失败，发生 IOException");
			e.printStackTrace();
		}

	}

	/** 
	 * 关闭ZK连接 
	 */
	public void releaseConnection() {
		if (zk != null) {
			try {
				this.zk.close();
			} catch (InterruptedException e) {
				// ignore 
				e.printStackTrace();
			}
		}
	}

	/** 
	*  创建节点 
	* @param path 节点path 
	* @param data 初始数据内容 
	* @return 
	*/
	public boolean createPath(String path, String data) {
		try {
			zk.create(path, 
					data.getBytes(), 
					Ids.OPEN_ACL_UNSAFE, 
					CreateMode.PERSISTENT);
			System.out.println("节点创建成功, Path: " +path+ " value: " + data);
		} catch (KeeperException e) {
			System.out.println("节点创建失败，发生KeeperException");
			e.printStackTrace();
		} catch (InterruptedException e) {
			System.out.println("节点创建失败，发生 InterruptedException");
			e.printStackTrace();
		}
		return true;
	}

	/** 
	 * 更新指定节点数据内容 
	 * @param path 节点path 
	 * @param data  数据内容 
	 * @return 
	 */
	public boolean writeData(String path, String data) {
		try {
			System.out.println("更新数据成功，path：" + path + ", stat: " + this.zk.setData(path, data.getBytes(), -1));
			return true;
		} catch (KeeperException e) {
			System.out.println("更新数据失败，发生KeeperException，path: " + path);
			e.printStackTrace();
		} catch (InterruptedException e) {
			System.out.println("更新数据失败，发生 InterruptedException，path: " + path);
			e.printStackTrace();
		}
		return false;
	}
	
	  /** 
     * 删除指定节点 
     * @param path 节点path 
     */ 
    public void deleteNode( String path ) { 
        try { 
            this.zk.delete( path, -1 ); 
            System.out.println( "删除节点成功，path：" + path ); 
        } catch ( KeeperException e ) { 
            System.out.println( "删除节点失败，发生KeeperException，path: " + path  ); 
            e.printStackTrace(); 
        } catch ( InterruptedException e ) { 
            System.out.println( "删除节点失败，发生 InterruptedException，path: " + path  ); 
            e.printStackTrace(); 
        } 
    } 
    
    
    /** 
     * 删除指定节点 
     * @param path 节点path 
     */ 
    public void deleteNodeRecursion( String path ) { 
        try { 
        	
        	
        	List<String> paths=zk.getChildren(path, false);
        	for (String p:paths){
        		deleteNodeRecursion(path+"/"+p);
                System.out.println(path+"/"+p);
            }
        	 for(String p:paths){
                 zk.delete(path+"/"+p, -1);
             }
            
        } catch ( KeeperException e ) { 
            System.out.println( "删除节点失败，发生KeeperException，path: " + path  ); 
            e.printStackTrace(); 
        } catch ( InterruptedException e ) { 
            System.out.println( "删除节点失败，发生 InterruptedException，path: " + path  ); 
            e.printStackTrace(); 
        } 
    } 

	/** 
	 * 读取指定节点数据内容 
	 * @param path 节点path 
	 * @return 
	 */
	public String readData(String path) {
		try {
			byte[] bbb = zk.getData(path, false, null);
			String rs = new String(bbb, "utf8");
			System.out.println("path  :"+path+"::"+rs);
			return rs;
		} catch (KeeperException e) {
			System.out.println("读取数据失败，发生KeeperException，path: " + path);
			e.printStackTrace();
			
		} catch (InterruptedException e) {
			System.out.println("读取数据失败，发生 InterruptedException，path: " + path);
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			
		}
		return "";
	}

	@Override
	public void process(WatchedEvent event) {
		 if (event.getState() == KeeperState.SyncConnected)
		    {
		      System.out.println("watcher received event");
		      countDownLatch.countDown();
		    }

	}
}
