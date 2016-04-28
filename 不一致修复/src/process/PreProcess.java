package process;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * 预处理：对输入的表intxt进行标志话，每行前面加上自己的主键（偏移量或者行号），
 * 防止reduce输出无序打乱原intxt顺序，使得ResolvedMap中的mapkey（偏移&属性列号）无法相比较
 * 添加后格式（一行）：偏移量#tuple  如：12#lily    22    Ave Street    150001
 */
public class PreProcess {

public static class PreProcessMapper extends Mapper<Object, Text, Text, Text>{
		
		private Text thisKey = new Text();		  
		
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {			
			thisKey.set(key.toString());
			context.write(thisKey, value);
		}
	}


	public static class PreProcessReducer extends Reducer<Text,Text,NullWritable,Text> { 
		
		private NullWritable nullkey = NullWritable.get(); 
		private Text thisvalue = new Text(); 

		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			for (Text value : values) {
				String newline = key.toString() + "#" + value.toString();
				thisvalue.set(newline);
				context.write(nullkey, thisvalue);
			}			
									
		}
	}
	
	public static boolean execute(String inPath, String outPath) 
			throws IOException,InterruptedException, ClassNotFoundException{
		
		boolean ret = true;
		Configuration conf = new Configuration();
		Job job = new Job(conf, "PreProcess");   
		job.setJarByClass(PreProcess.class);	    
	    job.setMapperClass(PreProcessMapper.class);
	    job.setReducerClass(PreProcessReducer.class);
	    
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class); 
	    
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	    
	    FileInputFormat.addInputPath(job, new Path(inPath));	    
	    FileOutputFormat.setOutputPath(job, new Path(outPath));
	    ret = job.waitForCompletion(true);
	    return ret;
	}
	public static void main(String[] args) throws Exception {
		PreProcess.execute("testinput/f0.txt", "PreProcess");
	}

}
