package com.lixf.hadoop.dome.mapreduce;

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
  * @Description: 
  * @Company:www.szmsd.com
  * @author:Administrator
  * @date:2016年10月24日 下午10:52:11
 */
public class WordCount {
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(WordCount.class);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		FileInputFormat.setInputPaths(job, new Path("/input/words.txt"));
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileOutputFormat.setOutputPath(job, new Path("/root/wcout"));
	}
	
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
