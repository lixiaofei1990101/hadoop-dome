package com.lixf.hadoop.dome.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
  * @ClassName: GPRSSort
  * @Description: 手机上网上行流量进行排序处理
  * @Company:www.szmsd.com
  * @author:Li Xiao Fei
  * @date:2016年12月1日 下午2:28:05
 */
public class GPRSSort {
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(GPRSSort.class);
		job.setMapperClass(SortMapper.class);
		//当mapper的输出key和value和Reducer输出类型一致的时候下面这个地方可以省略掉
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		//文件系统中要输入到map中的文件地址路径（/input/datacount.dat）
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.10.200:9000/data/input/GPRS.dat"));
		job.setReducerClass(SortReduce.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		//reduce执行完成之后的结果输出的路径（/output/grrscount）
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.10.200:9000/data/output/sort"));
		
		job.waitForCompletion(true);
	}
	
	public static class SortMapper extends Mapper<LongWritable, Text, IntWritable, Text>{

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split("\t");
			String telNo = String.valueOf(fields[1]);
			int up = Integer.valueOf(fields[8]);
			context.write(new IntWritable(up), new Text(telNo));
		}
		
	}
	
	public static class SortReduce extends Reducer<IntWritable, Text, IntWritable, Text>{
		
		private IntWritable sortNo = new IntWritable(1);

		@Override
		protected void reduce(IntWritable key, Iterable<Text> value,
				Reducer<IntWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			for (Text longWritable : value) {
				String temp = longWritable.toString();
				int sortData = key.get();
				String temp1 = temp+" "+String.valueOf(sortData);
				context.write(sortNo, new Text(temp1));
				sortNo = new IntWritable(sortNo.get()+1);
			}
		}
		
		
	}
}
