package process;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * 后处理：对修复后的结果去标志，恢复为本来intxt的格式。
 * 如：12#lily    22    Ave Street    150001 恢复为 lily    22    Ave Street    150001
 */
public class PostProcess {

public static class PostProcessMapper extends Mapper<Object, Text, Text, NullWritable>{
		
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {			
			context.write(new Text(value.toString().split("#")[1]),NullWritable.get());
		}
	}

	public static boolean execute(String inPath, String outPath) 
			throws IOException,InterruptedException, ClassNotFoundException{
		
		boolean ret = true;
		Configuration conf = new Configuration();
		Job job = new Job(conf, "PostProcess");   
		job.setJarByClass(PostProcess.class);	    
	    job.setMapperClass(PostProcessMapper.class);
	    
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(NullWritable.class); 
	    
	    FileInputFormat.addInputPath(job, new Path(inPath));	    
	    FileOutputFormat.setOutputPath(job, new Path(outPath));
	    ret = job.waitForCompletion(true);
	    return ret;
	}
	public static void main(String[] args) throws Exception{
		execute("/home/ning/桌面/out_RepairUnconsist/output/final/part-r*", "PostProcess");

	}

}
