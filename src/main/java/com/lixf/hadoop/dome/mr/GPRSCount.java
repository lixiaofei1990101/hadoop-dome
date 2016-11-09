package com.lixf.hadoop.dome.mr;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.lixf.hadoop.dome.mr.entity.DataBean;

/**
 * 
  * @ClassName: GPRSCount
  * @Description: 用于统计手机上网产生的流量
  * @Company:www.szmsd.com
  * @author:Administrator
  * @date:2016年11月2日 下午9:45:48
 */
public class GPRSCount {
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(GPRSCount.class);
		job.setMapperClass(WCMapper.class);
		//当mapper的输出key和value和Reducer输出类型一致的时候下面这个地方可以省略掉
		//job.setMapOutputKeyClass(Text.class);
		//job.setMapOutputValueClass(DataBean.class);
		//文件系统中要输入到map中的文件地址路径（/input/datacount.dat）
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		job.setReducerClass(WCReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DataBean.class);
		//reduce执行完成之后的结果输出的路径（/output/grrscount）
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setPartitionerClass(ProviderPartitioner.class);
		job.setNumReduceTasks(Integer.parseInt(args[2]));
		
		job.waitForCompletion(true);
	}
	
	public static class WCMapper extends Mapper<LongWritable, Text, Text, DataBean>{

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DataBean>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split("\t");
			String telNo = fields[1];
			long up = Long.parseLong(fields[8]);
			long down = Long.parseLong(fields[9]);
			DataBean bean = new DataBean(telNo, up, down);
			context.write(new Text(telNo), bean);
		}
		
	}
	
	public static class WCReduce extends Reducer<Text, DataBean, Text, DataBean>{

		@Override
		protected void reduce(Text key, Iterable<DataBean> v2, Reducer<Text, DataBean, Text, DataBean>.Context context)
				throws IOException, InterruptedException {
			long up_sum = 0;
			long down_sum = 0;
			for (DataBean dataBean : v2) {
				up_sum += dataBean.getUpPayLoad();
				down_sum += dataBean.getDownPayLoad();
			}
			DataBean bean = new DataBean("", up_sum, down_sum);
			context.write(key, bean);
		}
	}
	/**
	  * @ClassName: ProviderPartitioner
	  * @Description: 数据分区处理
	  * @Company:www.szmsd.com
	  * @author:Administrator
	  * @date:2016年11月9日 下午9:29:05
	 */
	public static class ProviderPartitioner extends Partitioner<Text, DataBean>{
		private static Map<String, Integer> provierMap = new HashMap<String,Integer>();
		static{
			provierMap.put("135", 1);
			provierMap.put("136", 1);
			provierMap.put("137", 1);
			provierMap.put("138", 1);
			provierMap.put("139", 1);
			provierMap.put("150", 2);
			provierMap.put("159", 2);
			provierMap.put("182", 3);
			provierMap.put("183", 3);
		}
		@Override
		/**
		 * 进行分区，返回分区号
		 * 将前面统计的数据按照运营商进行分区处理
		 */
		public int getPartition(Text key, DataBean value, int numPartitions) {
			String account = key.toString();
			String subValue = account.substring(0, 3);
			Integer code = provierMap.get(subValue);
			if (code == null) {
				code = 0;
			}
			return code;
		}
		
	}
}
