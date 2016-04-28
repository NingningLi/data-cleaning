package repair;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RepairVariable extends Configured implements Tool {

	  public static class Var_ViolateMapper extends Mapper<Object, Text, Text, Text>{
		  		   
		  private Text thisKey = new Text();
		  private Text thisValue = new Text();
  
		  public void map(Object key, Text value, Context context) 
				  throws IOException, InterruptedException {
			  String[] total = value.toString().split("\t");
			  thisKey.set(total[0]);
			  thisValue.set(total[1]);
			  context.write(thisKey, thisValue);
		  }
	  }
	  
	  public static class Var_ViolateReducer extends Reducer<Text,Text,NullWritable,Text> { 
			
			private Text thisValue = new Text();
			private NullWritable nullkey = NullWritable.get(); 
			private String inweight = "";
			private ArrayList<ArrayList<String>> cfds = new ArrayList<ArrayList<String>>();
			@Override
			protected void setup(org.apache.hadoop.mapreduce.Reducer<Text,Text,NullWritable,Text>.Context context) throws IOException ,InterruptedException {
				Configuration conf = context.getConfiguration();
				inweight = conf.get("inweight","1,0.9,0.9,0.8,0.8,0.8,0.8");
				Path[] caches=DistributedCache.getLocalCacheFiles(context.getConfiguration());
				if(caches==null||caches.length<=0){
					System.exit(1);
					} 
				@SuppressWarnings("resource")
				BufferedReader br=new BufferedReader(new FileReader(caches[0].toString()));
				String line ;
		        ArrayList<String> cfd1 = null;
		      
		        while((line=br.readLine())!=null){
		        	cfd1 = new ArrayList<String>();
		        	if(line.startsWith("%"))
		        		break;
		        	if(line.startsWith("$")){
		        		line=line.substring(1);
		            	String[] str = line.split("#");
		            	for(int i=0;i<str.length;i++){
		            		cfd1.add(str[i]);
		            		}
		            	cfds.add(cfd1);
		        	}
		        }
				super.setup(context);
			};
			
			public void reduce(Text key, Iterable<Text> values, Context context) 
					throws IOException, InterruptedException {
				
				
				String[] t = key.toString().split("&");
				String offset = t[0];
				String[] tuple = t[1].split(",");
				boolean _zero = false;
				boolean _one = false;
				Vector<String> solution = new Vector<String>();
				
				for (Text val : values) {
					String[] info = val.toString().split(",");
					String tag = info[0];
					
					if(tag.equals("0")){
						_zero = true;
						solution.add(val.toString());
					}
					else
						_one = true;					
				}
					
				
				String tab_tuple = new String();
				for(int a = 0; a < tuple.length; a++){
					if(a == tuple.length - 1)
						tab_tuple += tuple[a];
					else
						tab_tuple += tuple[a] + "\t";
				}
				if(_zero == false && _one == true){
					thisValue.set(offset + "#" + tab_tuple);
					context.write(nullkey, thisValue);
				}					
				if(_zero == true ){
					Vector<String> aa = new Vector<String>();
					Vector<String> bb = new Vector<String>();
					for (String str : solution) {
						String[] xx = str.split(",");
						String attr_index = xx[1];
						String fixValue = xx[2];
						int cfd_index = Integer.parseInt(xx[3]);
						String mapkey = offset + "&" + attr_index;
						if(!Resolved.ResolvedMap.containsKey(mapkey)){
							aa.add(attr_index);
							bb.add(fixValue);
							Resolved.ResolvedMap.put(mapkey, fixValue);
							
						}							
						if(Resolved.ResolvedMap.containsKey(mapkey) && !Resolved.ResolvedMap.get(mapkey).equals(fixValue)){
							
							double temp = 2;						
							int min = 0;
							ArrayList<String> cfd1 = cfds.get(cfd_index);
							String cfd = cfd1.get(0);
							
							
							int leftsize = cfd.split("@")[0].split(",").length;
							for(int i = 0; i < leftsize; i++){
								int leftindex = Integer.parseInt(cfd1.get(cfd1.size()-1).split(",")[i]);				
								double cur_weight = Double.parseDouble(inweight.split(",")[leftindex]);
								if(temp > cur_weight){
									temp = cur_weight;
									min = leftindex;
								}
							}
							aa.add(Integer.toString(min));
							bb.add("?");
							String addkey = offset + "&" + min;
							Resolved.ResolvedMap.put(addkey, "?");
							
							
							
							
							java.util.Random r=new java.util.Random();
							Resolved.p = Resolved.p * (r.nextDouble()%1.0);
							
						}													
					}
					
					for (int i = 0; i < aa.size(); i++) {
						int m = Integer.parseInt(aa.get(i));
						tuple[m] = bb.get(i);
					}
					String fixed = new String();
					for(int i = 0; i < tuple.length; i++){
						if(i != tuple.length - 1)
							fixed += tuple[i] + "\t";
						else
							fixed += tuple[i];
					}
					thisValue.set(offset + "#" + fixed);
					context.write(nullkey, thisValue);
				}
									
			}
		}
	  
	  public int run(String args[]) throws Exception{
		  Configuration conf = new Configuration();
		  FileSystem fs = FileSystem.get(conf);
		  fs.delete(new Path(args[1]), true);
		  
		  getConf().addResource(new Path(args[2]));
			String cfd1 = getConf().get("in.cfd","/home/ning/桌面/input/cfd.txt");
			DistributedCache.addCacheFile(new Path(cfd1).toUri(),conf);
			
			String inweight =getConf().get("in.weight1","1,0.9,0.9,0.8,0.8,0.8,0.8");
			conf.set("inweight",inweight);
			
			Job job = new Job(conf, "Repair Variable Violate");   
			job.setJarByClass(RepairVariable.class);
			job.setMapperClass(Var_ViolateMapper.class);
			job.setReducerClass(Var_ViolateReducer.class);
		    
		    
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class); 
		    
		    job.setOutputKeyClass(NullWritable.class);
		    job.setOutputValueClass(Text.class);
			
			int reduce_task_number = conf.getInt("num.reduceTask", 2);
			job.setNumReduceTasks(reduce_task_number);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));	    
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    
		    int res = job.waitForCompletion(true) ? 0 : 1;
			if (res == 0) {
				System.out.println("RepairVariable done.");
			} else {
				System.out.println("RepairVariable failed.");
			}

			return res;
		}
	  public static void main(String[] args) throws Exception {
		  	String[] path= new String[3];
			path[0]="out_RepairUnconsist/output/VarGroup/part-r-00000";
			path[1]="output";
			path[2]="/home/ning/桌面/configuration.xml";
			int res = ToolRunner.run(new Configuration(), new RepairVariable(), path);
			System.exit(res);
		}
}
