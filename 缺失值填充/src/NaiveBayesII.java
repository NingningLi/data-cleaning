import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;




public class NaiveBayesII extends Configured implements Tool {
	public static class ImpMap extends Mapper<Object,Text,Text,Text>{
		public void map(Object key,Text value,Context context)throws IOException,InterruptedException{
			String line=value.toString();

			if(line.contains("#")){
				Text key2=new Text();
				Text value2=new Text();
				int sep=line.indexOf("#");
				String k2=line.substring(0, sep-1);
				String v2=line.substring(sep);
				key2.set(k2);
				value2.set(v2);
				context.write(key2, value2);
			}
			else{
				String str = value.toString().split("\t")[1].replaceAll( "\\|","\t");

				context.write(new Text(key.toString()), new Text(str));
			}
		}
	}
	public static class ImpReduce extends Reducer<Text,Text,Text,Text>{
		private int cNo;
		private long no=0;
		
		protected void setup(Context context)throws IOException,InterruptedException{
			Path[] caches = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			@SuppressWarnings("resource")
			BufferedReader br = new BufferedReader(new FileReader(caches[0].toString()));
			String line;
			
			line=br.readLine();
			cNo = Integer.parseInt(line.split("#")[0]);
             super.setup(context);
		}
		public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException{
			HashMap<String,Double> classfy=new HashMap<String,Double>();
			HashMap<String,Integer> pCount=new HashMap<String,Integer>();
			String record="";
			for(Text val:values){
				String value=val.toString();
				if(!value.startsWith("#")){
					record=value;
					continue;
				}
				StringTokenizer token=new StringTokenizer(value,"#");
				while(token.hasMoreTokens()){
					String[] ele=token.nextToken().split(",");
					String c=ele[0];
					Double p=Double.parseDouble(ele[1]);
					if(classfy.containsKey(c)){
						classfy.put(c, classfy.get(c)*p);
						pCount.put(c, pCount.get(c)+1);
					}
					else{
						Double pro=Double.parseDouble(ele[2]);
						classfy.put(c, p*pro);
						pCount.put(c, new Integer(1));
					}
				}
			}
			Double max=new Double(0);
			String imputeValue="?";
			int maxSize=0;
			for(String c:classfy.keySet()){
				if(pCount.get(c)<maxSize)
					continue;
				else 
					maxSize=pCount.get(c);
				if(max<classfy.get(c)){
					max=classfy.get(c);
					imputeValue=c;
				}
			}
			String[] ele=record.toString().split("\t");
			if(!"?".equals(ele[cNo])){
				String str = record.replaceAll("\t", "\\|");
				context.write(new Text(++no+"\t"+str), new Text(""));
			}
			else{
				StringBuffer sb=new StringBuffer();
				sb.append(++no+"\t");
				ele[cNo]=imputeValue;
				sb.append(ele[0]);
				for(int i=1;i<ele.length;i++){
					sb.append("|"+ele[i]);
				}
				context.write(new Text(sb.toString()), new Text(""));
			}
		}
	}
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf=new Configuration();
		getConf().addResource(args[0]);
		
		String possibleValue = conf.get("PossibleValue","input/possibleValue");
		DistributedCache.addCacheFile(new Path(possibleValue).toUri(), conf);
		
		Job job=new Job(conf,"ImputeMR");
		job.setJarByClass(NaiveBayesII.class);
		job.setMapperClass(ImpMap.class);
		job.setReducerClass(ImpReduce.class);
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileInputFormat.addInputPath(job, new Path(args[2]));
		FileOutputFormat.setOutputPath(job, new Path("PostProcess"));
		if(job.waitForCompletion(true)){
			System.out.println("NaiveBayesII done.");
			return 0;
		}else{
			System.out.println("NaiveBayesII failed.");
			return 1;
		}
	}
}
