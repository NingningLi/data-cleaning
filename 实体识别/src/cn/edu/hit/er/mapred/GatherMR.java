package cn.edu.hit.er.mapred;

import java.io.IOException;
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

/**
 * 将各个属性的计算结果进行合并
 * @author hr1
 * 
 */
public class GatherMR extends Configured implements Tool {

	public static class GatherMRMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(
				LongWritable key,
				Text value,
				org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String k = line.substring(0, line.indexOf(":"));
			String v = line.substring(line.indexOf(":") + 2);
			
			context.write(new Text(k), new Text(v));

		}

	}

	public static class GatherMRReducer extends
			Reducer<Text, Text, NullWritable, Text> {

		private static float limit = 0f;

		/*
		 *读入org.er.limit参数
		 */
		@Override
		protected void setup(
				org.apache.hadoop.mapreduce.Reducer<Text, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			limit = context.getConfiguration().getFloat("org.er.limit", 0.003f);
		};

		@Override
		protected void reduce(
				Text key,
				java.lang.Iterable<Text> values,
				org.apache.hadoop.mapreduce.Reducer<Text, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {

			float total = 0;
			
			for (Text value : values) {

				total += Float.parseFloat(value.toString());
			}
			String s = key.toString().replace(",", " ");
			s = s + " " + Float.toString(total);
			
			if (total > limit)
				context.write(NullWritable.get(), new Text(s));
		};
	}

	public static void main(String[] args) throws Exception {

		String workdir = "EntityRecognition/";

		String argv[] = new String[11 + 1];
		for (int i = 0; i < 11; ++i) {
			argv[i] = workdir + "The_" + Integer.toString(i + 1)
					+ "th_property_out3";
		}
		argv[11] = workdir + "res";

		int res = ToolRunner.run(new Configuration(), new GatherMR(), argv);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
		Job job = new Job(conf, "GatherMR");
		job.setJarByClass(GatherMR.class);

		job.setMapperClass(GatherMRMapper.class);
		job.setReducerClass(GatherMRReducer.class);

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
		
		
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		int res = job.waitForCompletion(true) ? 0 : 1;
		if (res == 0) {
			System.out.println("Gather done.");
		} else {
			System.out.println("Gather failed.");
		}

		return res;
	}
}
