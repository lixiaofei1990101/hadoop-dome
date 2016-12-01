package com.lixf.hadoop.dome.base.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;
/**
 * 
  * @ClassName: HdfsUtils
  * @Description: hdfs工具类
  * @Company:www.szmsd.com
  * @author:Li Xiao Fei
  * @date:2016年11月30日 下午4:29:45
 */
public class HdfsUtils {

	public static final String FSURI = "hdfs://hadoop-master:9000";
	/**
	  * @Title: getFileSystem
	  * @Description: 获取FileSystem文件操作对象
	  * @return
	  * @throws Exception 
	  * @return FileSystem 
	  * @throws
	 */
	public static FileSystem getFileSystem() throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", FSURI);
		FileSystem fileSystem = FileSystem.get(new URI(FSURI), conf);
		return fileSystem;
	}
	
	/**
	  * @Title: readHDFSFile
	  * @Description:读取文件信息 
	  * @param hdfsUri
	  * @throws Exception 
	  * @return void 
	  * @throws
	 */
	public static void readHDFSFile(String hdfsUri) throws Exception {
		FileSystem fileSystem = getFileSystem();
		FSDataInputStream inStream = fileSystem.open(new Path(hdfsUri));
		try {
			IOUtils.copyBytes(inStream, System.out, 4096, false);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(inStream);
		}
	}
	/**
	  * @Title: writeHDFSFile
	  * @Description: 将本地文件写到hdfs文件系统
	  * @param hdfsUri
	  * @param localFile
	  * @throws Exception 
	  * @return void 
	  * @throws
	 */
	public static void writeHDFSFile(String hdfsUri, String localFile)
			throws Exception {
		FileSystem fileSystem = getFileSystem();
		FSDataOutputStream outStream = fileSystem.create(new Path(hdfsUri));
		FileInputStream inStream = new FileInputStream(new File(localFile));
		try {
			IOUtils.copyBytes(inStream, outStream, 4096, false);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(inStream);
			IOUtils.closeStream(outStream);

		}
	}

	/**
	  * @Title: uploadFile
	  * @Description: 上传文件到hdfs文件系统
	  * @param localFile
	  * @param hdfsUri
	  * @throws Exception 
	  * @return void 
	  * @throws
	 */
	public static void uploadFile(String localFile, String hdfsUri)
			throws Exception {
		FileSystem fileSystem = getFileSystem();
		Path srcPath = new Path(localFile);
		Path dstPath = new Path(hdfsUri);
		// 注意有好多几个重载的方法copyFromLocalFile
		fileSystem.copyFromLocalFile(srcPath, dstPath);
	}
	/**
	  * @Title: downloadFile
	  * @Description: 下载文件系统的文件到本地
	  * @param localFile
	  * @param hdfsUri
	  * @throws Exception 
	  * @return void 
	  * @throws
	 */
	public static void downloadFile(String localFile, String hdfsUri)
			throws Exception {
		FileSystem fileSystem = getFileSystem();
		FSDataInputStream inStream = fileSystem.open(new Path(hdfsUri));
		FileOutputStream outStream = new FileOutputStream(new File(localFile));
		try {
			IOUtils.copyBytes(inStream, outStream, 4096, false);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(inStream);
			IOUtils.closeStream(outStream);
		}

	}
	/**
	  * @Title: reNameFile
	  * @Description: 从命名文件系统文件
	  * @param hdfsSrcUri 需更改的文件
	  * @param hdfsDstUri 更改后的文件
	  * @throws Exception 
	  * @return void 
	  * @throws
	 */
	public static void reNameFile(String hdfsSrcUri, String hdfsDstUri)
			throws Exception {
		FileSystem fileSystem = getFileSystem();
		Path srcPath = new Path(hdfsSrcUri);
		Path dstPath = new Path(hdfsDstUri);
		fileSystem.rename(srcPath, dstPath);
	}
	/**
	  * @Title: delFile
	  * @Description: 删除文件
	  * @param hdfsUri 删除文件的路径
	  * @throws Exception 
	  * @return void 
	  * @throws
	 */
	public static void delFile(String hdfsUri) throws Exception {
		FileSystem fileSystem = getFileSystem();
		Path srcPath = new Path(hdfsUri);
		fileSystem.delete(srcPath, false);

	}
	/**
	  * @Title: mkDir
	  * @Description: 创建文件夹
	  * @param hdfsDirUri 创建的路径
	  * @throws Exception 
	  * @return void 
	  * @throws
	 */
	public static void mkDir(String hdfsDirUri) throws Exception {
		FileSystem fileSystem = getFileSystem();
		Path hdfsDirPath = new Path(hdfsDirUri);
		fileSystem.mkdirs(hdfsDirPath);
	}
	/**
	  * @Title: delDir
	  * @Description: 删除文件并递归删除
	  * @param hdfsDirUri 删除的文件路径
	  * @throws Exception 
	  * @return void 
	  * @throws
	 */
	public static void delDir(String hdfsDirUri) throws Exception {
		FileSystem fileSystem = getFileSystem();
		Path hdfsDirPath = new Path(hdfsDirUri);
		fileSystem.delete(hdfsDirPath, true);
	}
	
	/**
	  * @Title: getFileListInfo
	  * @Description: 获取文件列表信息
	  * @param hdfsDirUri 路径
	  * @throws Exception 
	  * @return void 
	  * @throws
	 */
	public static void getFileListInfo(String hdfsDirUri) throws Exception {
		FileSystem fs = getFileSystem();
		Path hdfsDirPath = new Path(hdfsDirUri);
		RemoteIterator<LocatedFileStatus> rit = fs.listFiles(hdfsDirPath, true);
		while (rit.hasNext()) {
			System.out.println("############################");
			LocatedFileStatus lfs = rit.next();
			System.out.println("Path:" + lfs.getPath());
			System.out.println("isDirectory:" + lfs.isDirectory());
			System.out.println("isFile:" + lfs.isFile());
			System.out.println("isFile:" + lfs.isFile());
			System.out.println("Owner:" + lfs.getOwner());
			System.out.println("Len:" + lfs.getLen());
			System.out.println("Replication():" + lfs.getReplication());
			BlockLocation[] bls = lfs.getBlockLocations();
			for (BlockLocation bl : bls) {
				System.out.println("Hosts:");
				for (String host : bl.getHosts()) {
					System.out.println(host);
				}
				System.out.println("Names:" + bl.getNames());
				for (String host : bl.getNames()) {
					System.out.println(host);
				}
				System.out.println("Length:" + bl.getLength());
				System.out.println("Offset:" + bl.getOffset());
			}
		}
	}
	
	/**
	  * @Title: getDNInfo
	  * @Description: 获取datanode信息
	  * @throws Exception 
	  * @return void 
	  * @throws
	 */
	public static void getDNInfo() throws Exception {
		FileSystem fs = getFileSystem();
		DistributedFileSystem hdfs = (DistributedFileSystem) fs;
		DatanodeInfo[] dnInfo = hdfs.getDataNodeStats();
		for (DatanodeInfo info : dnInfo) {
			System.out.println("###################");
			System.out.println("HostName:" + info.getHostName());
			System.out.println("InfoAddr:" + info.getInfoAddr());
			System.out.println("InfoPort:" + info.getInfoPort());
			System.out.println("Name:" + info.getName());
			System.out.println("DfsUsed:" + info.getDfsUsed());
		}
	}

	/**
	  * @Title: getFileInDnPosition
	  * @Description: 查找文件在集群中的位置
	  * @param hdfsUri
	  * @throws Exception 
	  * @return void 
	  * @throws
	 */
	public static void getFileInDnPosition(String hdfsUri) throws Exception {
		FileSystem fs = getFileSystem();
		Path hdfsUriPath = new Path(hdfsUri);
		FileStatus fstatus = fs.getFileStatus(hdfsUriPath);
		BlockLocation[] bls = fs.getFileBlockLocations(fstatus, 0,fstatus.getLen());
		int blockLen = bls.length;
		for (int i = 0; i < blockLen; ++i) {
			String[] hosts = bls[i].getHosts();
			System.out.println("block_" + i + "_location:" + hosts[i]);
			System.out.println("Length:"+ bls[i].getLength());
			System.out.println("Offset:"+ bls[i].getOffset());
		}

	}
	/**
	  * @Title: createFileAndWriteStr
	  * @Description: 在文件系统中的文件追加字符串
	  * @param hdfsUri
	  * @param strInto
	  * @throws Exception 
	  * @return void 
	  * @throws
	 */
	public static void createFileAndWriteStr(String hdfsUri, String strInto)
			throws Exception {
		FileSystem fs = getFileSystem();
		Path path = new Path(hdfsUri);
		FSDataOutputStream out = fs.create(path);
		out.writeUTF(strInto);
		fs.close();
	}
	
	public static void putMerge(String LocalDir, String fsFile) throws Exception{
		Configuration  conf = new Configuration();
		FileSystem fs = getFileSystem(); 
		FileSystem local = FileSystem.getLocal(conf);
		Path localDir = new Path(LocalDir);
        Path HDFSFile = new Path(fsFile);
        FileStatus[] fileList =  local.listStatus(localDir);
        FSDataOutputStream out = fs.create(HDFSFile); 
        for (FileStatus fileStatus : fileList) {
        	Path temp = fileStatus.getPath();
            FSDataInputStream in = local.open(temp);
            IOUtils.copyBytes(in, out, 4096, false);    //读取in流中的内容放入out
            in.close(); //完成后，关闭当前文件输入流
		}
        out.close();
	}
}
