package cn.edu.hit.er.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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

import cn.edu.hit.er.util.MyComparator;



/**
 * 
 * 建立倒排表
 * @author hr1
 * 
 */
public class PreMR extends Configured implements Tool {


	public static class PreMRMapper extends Mapper<LongWritable, Text, Text, Text> {

		/*
		 * 
		 */
		@Override
		protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,Text>.Context context) throws IOException ,InterruptedException {
			
			String line = value.toString();
			
			String entityId = line.substring(0, line.indexOf("---"));
			String tmp[] = line.substring(line.indexOf("---") + 3).split("\\|");
			String[][] out;	
			String prefix = "";
			int num = context.getConfiguration().getInt("org.er.propertyNum",4);
			out = new String[num][];
			
			for(int ith = 0;ith < num;ith++){
				if (ith > tmp.length - 1) {
					return;
				}
				out[ith] = tmp[ith].split(",");
			}			
			for(int i = 0;i < num;i++){				
				for(String property : out[i]){
					prefix =Integer.toString(i)+"$"+property;
					
					context.write(new Text(prefix) , new Text(entityId));
				}
			}			
		}
	}
	
	public static class PreMRReducer extends Reducer<Text, Text, NullWritable, Text> {
		
		@Override
		protected void reduce(Text key, java.lang.Iterable<Text> values, org.apache.hadoop.mapreduce.Reducer<Text,Text,NullWritable,Text>.Context context) throws IOException ,InterruptedException {
		
			
			ArrayList<String> list = new ArrayList<String>();
			StringBuffer sb = new StringBuffer();
			boolean flag = false;
			String Key = key.toString();
			Key=Key.substring(0,Key.indexOf("$")+1);
			for (Text value : values) {
				
				list.add(value.toString());
				
			}
			Collections.sort(list, new MyComparator());
			
			for (String s : list) {
				if (flag) {
					sb.append(","+Key);
				}
				sb.append(s);
				flag = true;
			}
			
			Key = Key+sb.toString();
			
			context.write(NullWritable.get(), new Text(Key));
		};
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new PreMR(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		
		
		Configuration conf = getConf();
		
		
		Job job = new Job(conf, "PreMR");
		job.setJarByClass(PreMR.class);


		
		job.setMapperClass(PreMRMapper.class);
		job.setReducerClass(PreMRReducer.class);
		
		
		
		job.setMapOutputKeyClass(Text.class); 
		job.setMapOutputValueClass(Text.class);

		
		job.setOutputKeyClass(NullWritable.class);
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
			System.out.println("PreMR done.");
		
		} else {
			System.out.println("PreMR failed.");
		}

		return res;
	}
}
