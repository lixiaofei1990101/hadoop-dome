package com.lixf.hadoop.dome.base.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HdfsDome {

	public static void main(String[] args) throws Exception {
//		read();
		
//		write();
		
//		HdfsUtils.readHDFSFile("/data/input/upload");
		
//		HdfsUtils.writeHDFSFile("/data/input/words", "E:\\Apache\\hadoop_test_data\\words");
		
//		HdfsUtils.uploadFile("E:\\Apache\\hadoop_test_data\\words", "/data/input/words");
		
//		HdfsUtils.downloadFile("E:\\Apache\\hadoop_test_data\\words.txt", "/data/input/words");
		
//		HdfsUtils.reNameFile("/data/input/upload", "/data/input/upload.txt");
		
//		HdfsUtils.delFile("/data/input/upload.txt");
		
//		HdfsUtils.mkDir("/data/input/a");
		
//		HdfsUtils.delDir("/data/input/a");
//		
//		HdfsUtils.getFileListInfo("/data/input/words");
		
//		HdfsUtils.getFileInDnPosition("/data/input/words");
		
//		HdfsUtils.getDNInfo();
		
//		HdfsUtils.createFileAndWriteStr("/data/input/upload","你好,中国，chinese");
		
//		HdfsUtils.putMerge("E:\\Apache\\hadoop_test_data\\list", "/data/output/merge");
	}

	//重复上传同名文件会覆盖
	public static void write() throws IOException, URISyntaxException,
			InterruptedException {
		String uri = "/data/input/upload";
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoop-master:9000");
		FileSystem fileSystem = FileSystem.get(new URI(uri), conf);
		FSDataOutputStream outStream = fileSystem.create(new Path(uri));
		FileInputStream inStream = new FileInputStream(new File("E:\\Apache\\hadoop_test_data\\upload"));
		try {
			IOUtils.copyBytes(inStream, outStream, 4096, false);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(inStream);
			IOUtils.closeStream(outStream);
			
		}
	}

	public static void read() throws IOException, URISyntaxException,
			InterruptedException {
		String uri = "/data/input/test";
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoop-master:9000");
		FileSystem fileSystem = FileSystem.get(new URI(uri), conf);
		FSDataInputStream inStream = fileSystem.open(new Path(uri));
		try {
			IOUtils.copyBytes(inStream, System.out, 4096, false);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(inStream);
		}
	}

}
