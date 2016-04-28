import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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

public class NaiveBayesI extends Configured implements Tool {
public static class ProMapper extends Mapper<Object,Text,Text,Text>{
		
		private Text word=new Text();
		private Text key2=new Text();
		private ArrayList<String> possibleValue = new ArrayList<String>();
		private int cNo;
		private Integer[] exp;
		
		protected void setup(Context context)throws IOException,InterruptedException{
			Path[] caches = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			@SuppressWarnings("resource")
			BufferedReader br = new BufferedReader(new FileReader(caches[0].toString()));
			String line;
			
			 while((line=br.readLine())!=null){
				 if(line.contains("#")){
					 cNo = Integer.parseInt(line.split("#")[0]);
					 String[] tmp = line.split("#")[1].split(",");
					 exp = new Integer[tmp.length];
					 for(int i=0;i<tmp.length;i++){
						 exp[i] = Integer.parseInt(tmp[i]);
					 }
				 }else{
					 possibleValue.add(line);
				 }
			 }
             super.setup(context);
		}
		
		public void map(Object key,Text value,Context context)throws IOException,InterruptedException{
			String ele[]=value.toString().split("\t")[1].split("\\|");
			if(!"?".equals(ele[cNo])){
				key2.set(ele[cNo]);
				for(int i=0;i<exp.length;i++){
					int fNo=exp[i];
					if(!"?".equals(ele[fNo])){
						word.set(key+","+fNo+","+ele[fNo]);
						
						context.write(key2, word);
					}
				}
			}else{
				for(String tmp:possibleValue){
					key2.set(tmp);
					for(int i=0;i<exp.length;i++){
						int fNo=exp[i];
						if(!"?".equals(ele[fNo])){
							word.set("#"+key+","+fNo+","+ele[fNo]);
							
							context.write(key2, word);
						}
					}
				}
			}
		}
	}
	public static class ProReducer extends Reducer<Text,Text,Text,Text>{

		public void reduce(Text Key,Iterable<Text> values,Context context)throws IOException,InterruptedException{
			Map<String,RelatedInformation> likelihood=new HashMap<String,RelatedInformation>();
			Set<String> prioprob=new HashSet<String>();
			for(Text o:values){
				String str=o.toString();
				StringTokenizer token=new StringTokenizer(str,",");
				String row=token.nextToken();
				if(!row.startsWith("#")){
					prioprob.add(row);
					StringBuffer sb=new StringBuffer();
					while(token.hasMoreTokens()){
						sb.append(token.nextToken());
					}
					if(likelihood.containsKey(sb.toString())){		
						likelihood.get(sb.toString()).none_tuple.add(row);
					}
					else{
						likelihood.put(sb.toString(), new RelatedInformation());
						likelihood.get(sb.toString()).none_tuple.add(row);
					}
				}
				else{
					StringBuffer sb=new StringBuffer();
					while(token.hasMoreTokens()){
						sb.append(token.nextToken());
					}
					if(likelihood.containsKey(sb.toString())){		
						likelihood.get(sb.toString()).have_tuple.add(row);
						likelihood.get(sb.toString()).flag = true;
					}
					else{
						likelihood.put(sb.toString(), new RelatedInformation());
						likelihood.get(sb.toString()).have_tuple.add(row);
						likelihood.get(sb.toString()).flag = true;
					}
				}
			}
			int total=prioprob.size();
			for(String k:likelihood.keySet()){
				Float p=(float) likelihood.get(k).none_tuple.size();
				p/=total;
				if(p==0.0){
					p=(float) (1.0/total);
				}
				if(likelihood.get(k).flag==true){
					for(String key:likelihood.get(k).have_tuple){
						context.write(new Text(key.substring(1)),new Text("#"+Key.toString()+","+p.toString()+","+total));
					}
				}
			}
		}
	}
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf=new Configuration();
		getConf().addResource(args[0]);
		
		String possibleValue = conf.get("PossibleValue","input/possibleValue");
		DistributedCache.addCacheFile(new Path(possibleValue).toUri(), conf);
		
		Job job=new Job(conf,"ProMR");
		job.setJarByClass(NaiveBayesI.class);
		job.setMapperClass(ProMapper.class);
		job.setReducerClass(ProReducer.class);

		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		if(job.waitForCompletion(true)){
			System.out.println("NaiveBayesI done.");
			return 0;
		}else{
			System.out.println("NaiveBayesI failed.");
			return 1;
		}
	}
}
