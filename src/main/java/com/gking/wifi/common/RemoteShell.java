package com.gking.wifi.common;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

import ch.ethz.ssh2.ChannelCondition;
import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;

/**
* @author gking
* @date  2016年9月20日 
* 远程操作Shell脚本
*/
public class RemoteShell {

    private Connection conn; 
    private String hostport; 
    private String userName; 
    private String password;
    private String charset = Charset.defaultCharset().toString(); 
    private static final int TIME_OUT = 1000 * 5 * 60; 
    
    //Constructor
    public RemoteShell(String hostport, String userName, String password) {
        this.hostport = hostport;
        this.userName = userName;
        this.password = password;

    }
    
    //Log in
    
    public boolean login() throws IOException {
    	String[] ip_port = hostport.split(":");
    	if (ip_port.length == 2) {
    		String ip = ip_port[0];
    		int port = Integer.parseInt(ip_port[1]);
    		conn = new Connection(ip, port);
    	} else {
    		conn = new Connection(hostport);
    	}
    		
        conn.connect();
        return conn.authenticateWithPassword(userName, password);
    }
    
    //Execute the instruction
    public String exec(String cmds) {
        InputStream in = null;
        String result = "";
        try {
            if (this.login()) {
                Session session = conn.openSession(); // 打开一个会话
                session.execCommand("source /etc/profile;"+cmds);
//                session.execCommand(cmds);
                session.waitForCondition(ChannelCondition.EXIT_STATUS, TIME_OUT); 
                in = session.getStdout();
                result = this.processStdout(in, charset);
                session.close();
                conn.close();
            }
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        return result;
    }
    
    //???
    public String processStdout(InputStream in, String charset) {
        
        byte[] buf = new byte[1024];
        StringBuffer sb = new StringBuffer();
        try {
            while (in.read(buf) != -1) {
                sb.append(new String(buf, charset));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }
    
}
