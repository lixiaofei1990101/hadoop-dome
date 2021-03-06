package com.lixf.hadoop.dome.base.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

/**
 * 
  * @ClassName: RPCService
  * @Description: 服务端
  * @Company:www.szmsd.com
  * @author:Administrator
  * @date:2016年10月15日 下午3:57:21
 */
public class RPCService implements Bizable{
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
	    RPC.Server server = new RPC.Builder(conf).setProtocol(Bizable.class).setInstance(new RPCService()).setBindAddress("192.168.10.200").setPort(9527).build();
	    server.start();
	}

	public String getUser(String idCard) {
		if("123".equals(idCard)){
			return "张三";
		}else{
			return "李四";
		}
	}
	
}
