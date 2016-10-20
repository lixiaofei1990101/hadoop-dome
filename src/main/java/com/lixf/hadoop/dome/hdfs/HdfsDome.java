package com.lixf.hadoop.dome.hdfs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

/**
  * @ClassName: HdfsDome
  * @Description: 
  * @Company:www.szmsd.com
  * @author:Administrator
  * @date:2016年10月15日 下午2:41:37
 */
public class HdfsDome {
	
	private static FileSystem fs;
	/**
	  * @Title: init
	  * @Description: 初始化
	  * @throws Exception 
	  * @return void 
	  * @throws
	 */
	@Before
	public void init() throws Exception{
		fs = FileSystem.get(new URI("hdfs://hadoop-master:9000"), new Configuration(),"root");
	}
	/**
	  * @Title: getFile
	  * @Description: 下载文件
	  * @throws Exception 
	  * @return void 
	  * @throws
	 */
	@Test
	public void getFile() throws Exception{
		//hdfs文件系统中存放的路径
		InputStream input = fs.open(new Path("/input/jdk-7u76-linux-x64.tar.gz"));
		//下载到本地的路径
		OutputStream out = new FileOutputStream("E://jdk-1.7");
		IOUtils.copyBytes(input, out, 4096,true);
	}
	/**
	  * @Title: uploadFile
	  * @Description:上传文件 
	  * @throws Exception 
	  * @return void 
	  * @throws
	 */
	@Test
	public void uploadFile() throws Exception{
		InputStream in = new FileInputStream("E://hadoop-test-dir/redis.docx");
		OutputStream out = fs.create(new Path("/input/redis.docx"));
		IOUtils.copyBytes(in, out, 4096, true);
	}
	/**
	  * @Title: mkdirFile
	  * @Description: 创建文件
	  * @throws Exception 
	  * @return void 
	  * @throws
	 */
	@Test
	public void mkdirFile() throws Exception{
		boolean flag = fs.mkdirs(new Path("/test"));
		System.out.println(flag);
	}
	/**
	  * @Title: deleteFile
	  * @Description: 删除文件
	  * @throws Exception 
	  * @return void 
	  * @throws
	 */
	@Test
	public void deleteFile()throws Exception{
		//false表示不进行递归删除
		boolean flag = fs.delete(new Path("/test"), false);
		System.out.println(flag);
	}
}
