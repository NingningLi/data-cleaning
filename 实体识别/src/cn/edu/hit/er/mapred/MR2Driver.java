package cn.edu.hit.er.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * 
 * @author hr
 * 
 */
public class MR2Driver extends Configured implements Tool {

	public static Logger logger = Logger.getLogger(MR2Driver.class);

	public static class MR2Mapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,Text>.Context context) throws IOException ,InterruptedException {
			
			/*
			 * 输入：<"A","2">，输出为<A,"0:2">, 0代表冒号后的2为该key的权值
			 * 输入：<"A#B","2">，输出为<A,"1:A#B 2">,其中1代表冒号后的A#B 2为一个独立单元
			 */
			String line = value.toString();
			
			String tmp[] = line.split("\t");
			String k = tmp[0];
			String v = tmp[1];
			
			
			if (k.split("#").length == 1) {
				context.write(new Text(k), new Text("0:" + v));
				
			} else {
				
				String x = k.substring(0, k.indexOf("#"));
				
				context.write(new Text(x), new Text("1:" + line.replace("\t", ",")));
				
			}
			
		
		}

	}
	
	public static class MR2Reducer extends Reducer<Text, Text, Text, Text> {
		
		@Override
		protected void reduce(Text key, java.lang.Iterable<Text> values, org.apache.hadoop.mapreduce.Reducer<Text,Text,Text,Text>.Context context) throws IOException ,InterruptedException {
		
			/*
			 * 输入：A List<"0:2" "1:A#B,1" "1:A#C,1">
			 * 输出：A "0:2;1:A#B,1;1:A#C,1"
			 */
			StringBuffer sb = new StringBuffer();
			boolean flag = false;
			for (Text value : values) {
				if (flag) {
					sb.append(";");
				}
				sb.append(value.toString());
				flag = true;
			}
			context.write(key, new Text(sb.toString()));
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MR2Driver(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
		Job job = new Job(conf, "MR2");
		job.setJarByClass(MR2Driver.class);


		job.setMapperClass(MR2Mapper.class);
		job.setReducerClass(MR2Reducer.class);
		
		

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		
		int reduce_task_number = conf.getInt("org.er.num.reduceTask",
				2);
		job.setNumReduceTasks(reduce_task_number);

		
		
		
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		
		
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		int res = job.waitForCompletion(true) ? 0 : 1;
		if (res == 0) {
			System.out.println("MR2 done.");
		} else {
			System.out.println("MR2 failed.");
		}

		return res;
	}
}
