package com.lixf.hadoop.dome.mr.entity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
/**
  * @ClassName: DataBean
  * @Description: 数据封装类
  * @Company:www.szmsd.com
  * @author:Administrator
  * @date:2016年11月2日 下午9:48:31
 */
public class DataBean implements Writable {
	
	private String telNo;
	private long upPayLoad;
	private long downPayLoad;
	private long totalPayLoad;
	/**
	 * 序列化，把值序列化到流中
	 */
	public void write(DataOutput out) throws IOException {
		out.writeUTF(telNo);
		out.writeLong(upPayLoad);
		out.writeLong(downPayLoad);
		out.writeLong(totalPayLoad);
	}
	/**
	 * 反序列化,把流中的数据赋值给对象的属性
	 */
	public void readFields(DataInput in) throws IOException {
		this.telNo = in.readUTF();
		this.upPayLoad = in.readLong();
		this.downPayLoad = in.readLong();
		this.totalPayLoad = in.readLong();
	}

	public DataBean() {
		
	}
	
	public DataBean(String telNo, long upPayLoad, long downPayLoad) {
		this.telNo = telNo;
		this.upPayLoad = upPayLoad;
		this.downPayLoad = downPayLoad;
		this.totalPayLoad = upPayLoad + downPayLoad;
	}
	
	@Override
	public String toString() {
		return this.upPayLoad + "\t" +this.downPayLoad + "\t" + this.totalPayLoad;
	}
	
	public String getTelNo() {
		return telNo;
	}

	public void setTelNo(String telNo) {
		this.telNo = telNo;
	}

	public long getUpPayLoad() {
		return upPayLoad;
	}

	public void setUpPayLoad(long upPayLoad) {
		this.upPayLoad = upPayLoad;
	}

	public long getDownPayLoad() {
		return downPayLoad;
	}

	public void setDownPayLoad(long downPayLoad) {
		this.downPayLoad = downPayLoad;
	}

	public long getTotalPayLoad() {
		return totalPayLoad;
	}

	public void setTotalPayLoad(long totalPayLoad) {
		this.totalPayLoad = totalPayLoad;
	}
	
	

}
