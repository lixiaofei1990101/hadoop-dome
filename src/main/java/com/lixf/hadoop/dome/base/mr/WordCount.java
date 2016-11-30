package com.lixf.hadoop.dome.base.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
  * @ClassName: WordCount
  * @Description: 首先确定业务的逻辑，然后确定输入输出的样式
  * @Company:www.szmsd.com
  * @author:Administrator
  * @date:2016年10月24日 下午10:52:11
 */
public class WordCount {
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		//构建job对象
		Job job = Job.getInstance(conf);
		//main方法所在的类
		job.setJarByClass(WordCount.class);
		//设置Mapper的相关属性
		job.setMapperClass(MyMapper.class);
		//map输出key值得类型（String）
		job.setMapOutputKeyClass(Text.class);
		//map输出value值得类型
		job.setMapOutputValueClass(LongWritable.class);
		FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop-master:9000/data/input/test"));
		//设置reduce的相关属性
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop-master:9000/data/output/wordcount"));
		job.waitForCompletion(true);
	}
	/**
	  * @ClassName: MyMapper
	  * @Description: 数据拆解过程
	  * @Company:www.szmsd.com
	  * @author:Administrator
	  * @date:2016年10月26日 下午10:21:44
	 */
	private static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] words = line.split(" ");
			for(String w : words){
				context.write(new Text(w), new LongWritable(1));
			}
		}
		
	}
	/**
	  * @ClassName: MyReducer
	  * @Description: 数据的聚合过程
	  * @Company:www.szmsd.com
	  * @author:Administrator
	  * @date:2016年10月26日 下午10:21:59
	 */
	private static class MyReducer extends Reducer<Text, LongWritable,Text, LongWritable>{

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
			long counter = 0;
			for(LongWritable l : values){
				counter += l.get();
			}
			context.write(key, new LongWritable(counter));
		}
		
	}
}
