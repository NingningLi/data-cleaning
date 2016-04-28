package cn.edu.hit.er.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

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
import org.apache.log4j.Logger;

import cn.edu.hit.er.util.probability;

/**
 * 
 * @author hr
 * 
 */
public class MR3Driver extends Configured implements Tool {
	
	public static Logger logger = Logger.getLogger(MR3Driver.class);

	public static class MR3Mapper extends
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(
				LongWritable key,
				Text value,
				org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			/*
			 * 
			 * 输入：A "0:2;1:A#B,1;1:A#C,1" 输出： (1) A "0:2" (2) B "1:A,2;1:A#B,1"
			 * (3) C "1:A,2;1:A#C,1"
			 */

			String k = value.toString().split("\t")[0];

			String v[] = value.toString().split("\t")[1].split(";");
			Arrays.sort(v);

			String kNum = v[0].substring(2);

			for (String s : v) {
				if (s.startsWith("0:")) {   
					
					context.write(new Text(k), new Text(s));
				} else {

					String outK = s.substring(s.indexOf("#") + 1,
							s.indexOf(","));
					String outV = "1:" + k + "," + kNum + ";" + s;

					
					context.write(new Text(outK), new Text(outV));
				}
			}

		}

	}

	public static class MR3Reducer extends
			Reducer<Text, Text, NullWritable, Text> {

		private static float ratio = 0f;
		private static String config = "";
		@Override
		protected void setup(Context context)throws IOException,InterruptedException{
			config = context.getConfiguration().get("org.er.config","/home/ning/桌面/configuration.xml");
			super.setup(context);
		}
		protected void reduce(
				Text key,
				java.lang.Iterable<Text> values,
				org.apache.hadoop.mapreduce.Reducer<Text, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			
			ArrayList<String> list = new ArrayList<String>();
			String name2 = new String();
			Float t1 ;
			Float t2 = 0f;
			Float t3;
			int ith = 0;
			
			
			for (Text value : values) {
				String s = value.toString();

				if (!s.contains("#")) {					
					name2 = key.toString();
					name2 = name2.substring(key.toString().indexOf("$")+1);
					ith = Integer.parseInt(key.toString().substring(0,key.toString().indexOf("$")));
					t2 = Float.parseFloat(s.substring(s.indexOf(":") + 1));
					
				} else {
					
					list.add(s);
				}
			}
			String ss = context.getConfiguration().get("org.er.ratio", "0.25,0.25,0.25,0.25");
			ratio = Float.parseFloat(ss.split(",")[ith]);
			

			for (String s : list) {
				String[] tmp = s.split(";");
				
				String name1 = tmp[0].substring(tmp[0].indexOf(":") + 1,
						tmp[0].indexOf(","));
				name1 = name1.substring(key.toString().indexOf("$")+1);
				double t1Probability= probability.getProbability(name1,ith+1,config);
				double t2Probability= probability.getProbability(name2,ith+1,config);				
				double t3Probability= t1Probability*t2Probability;
				
				
				t1 = Float
						.parseFloat(tmp[0].substring(tmp[0].indexOf(",") + 1));
				t3 = Float
						.parseFloat(tmp[1].substring(tmp[1].indexOf(",") + 1));
				
				context.write(NullWritable.get(), new Text(name1 + "," +name2
						+ ": " + ratio * t3*t3Probability/(t1*t1Probability+t2*t2Probability) ));		
			}  			
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MR3Driver(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
		Job job = new Job(conf, "MR3");
		job.setJarByClass(MR3Driver.class);
		
		job.setMapperClass(MR3Mapper.class);
		job.setReducerClass(MR3Reducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		int reduce_task_number = conf.getInt("org.er.num.reduceTask", 2);
		job.setNumReduceTasks(reduce_task_number);
		
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		/*Path in = new Path("EntityRecognition/The_property_out2");
		Path out = new Path("EntityRecognition/output_out3");*/
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		int res = job.waitForCompletion(true) ? 0 : 1;
		if (res == 0) {
			System.out.println("MR3 done.");
		} else {
			System.out.println("MR3 failed.");
		}

		return res;
	}
}
