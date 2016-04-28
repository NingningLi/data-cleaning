package td;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class truthDiscovery extends Configured implements Tool{
public static class tdMapper extends Mapper<Object,Text,Text,Text>{
		
		public void map(Object key,Text value,Context context)throws IOException,InterruptedException{
			String ele[] = value.toString().split("\t");
			String Key = new String(ele[0]);
			String Value = new String(ele[1]);
			context.write(new Text(Key),new Text(Value));
		}
	}
	public static class tdReducer extends Reducer<Text,Text,Text,Text>{

		public void reduce(Text Key,Iterable<Text> values,Context context)throws IOException,InterruptedException{
			HashMap<String,Integer> knowledge = new HashMap<String,Integer>();
			for(Text o : values){
				String value = o.toString();
				if(!knowledge.containsKey(value))
					knowledge.put(value, new Integer(1));
				else
					knowledge.put(value,knowledge.get(value)+1);
			}
			int sum = 0;
			int max = 0;
			String mostpossible = new String();
			for(String str : knowledge.keySet()){
				int i = knowledge.get(str);
				sum += i;
				if(i > max){
					max = i;
					mostpossible = str;
				}
			}
			String value = mostpossible+"\t"+(Double.parseDouble(Integer.toString(max))/((sum)));
			System.out.println(Key.toString()+"\t"+value);
			context.write(Key,new Text(value));
		}
	}
	public int run(String[] args) throws Exception {
		getConf().addResource(args[0]);
		Configuration conf = getConf();
		
		
		Job job=new Job(conf,"TruthDiscovery");
		job.setJarByClass(truthDiscovery.class);
		job.setMapperClass(tdMapper.class);
		job.setReducerClass(tdReducer.class);
		int reducerNumber = conf.getInt("org.er.num.reduceTask",2);
		job.setNumReduceTasks(reducerNumber);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		if(job.waitForCompletion(true)){
			System.out.println("TruthDiscovery done.");
			return 0;
		}else{
			System.out.println("TruthDiscovery failed.");
			return 1;
		}
	}
}
