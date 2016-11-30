package com.lixf.hadoop.dome.base.hdfs;

import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

public class ChangeURLUtils {
	
	/**
	 *  注册，让Java程序识别HDFS URL形式
	 *  文件的位置是使用Hadoop Path呈现在Hadoop中的，与java.io中的不一样
	 */
	static {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}
	
	public static void readByUrl(String fileUrl) {
		// 打开输入流
		InputStream inStream = null;
		try {
			inStream = new URL(fileUrl).openStream();
			IOUtils.copyBytes(inStream, System.out, 4096, false);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(inStream);
		}
	}
}
