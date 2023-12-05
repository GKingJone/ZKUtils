import com.gking.wifi.zookeeper.ZookeeperUtil;

/**
* @author liliwei
* @date  2016年9月13日 
* 
*/
public class ZKUpdateTest {
	public static void main(String[] args) {

		ZookeeperUtil zkUtil = new ZookeeperUtil();
		zkUtil.updateData("cdh1", "/test/test1", "value2");
		
		System.out.println(zkUtil.getData("cdh1", "/test/test1"));

	}
}
