package com.lixf.hadoop.dome.rpc;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

/**
 * 
  * @ClassName: RPCClient
  * @Description: 客户端
  * @Company:www.szmsd.com
  * @author:Administrator
  * @date:2016年10月15日 下午4:24:26
 */
public class RPCClient{
	
	public static void main(String[] args) throws Exception{
		Bizable client = (Bizable)RPC.getProxy(Bizable.class, 10000, new InetSocketAddress("192.168.77.1", 9527), new Configuration());
	    String user = client.getUser("123");
	    System.out.println(user);
	}
	
}
