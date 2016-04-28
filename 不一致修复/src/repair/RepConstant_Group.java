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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RepConstant_Group extends Configured implements Tool {

	public static class Var_GroupMapper extends Mapper<Object, Text, Text, Text>{
		private ArrayList<ArrayList<String>> cfds = new ArrayList<ArrayList<String>>();
		@Override
		protected void setup(org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, Text>.Context context) throws IOException ,InterruptedException {
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
		
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {	
			
			int locate = value.toString().indexOf('#');			
			String offset = value.toString().substring(0, locate);
			String line = value.toString().substring(locate + 1);
			String[] tuple = line.split("\t");
			boolean _appear = false;
			boolean _firstLine = false;
			Text thisKey = new Text();
			Text thisValue = new Text();
			
			
			String transform = new String();
			for(int a = 0; a < tuple.length; a++){
				if(a == tuple.length - 1)
					transform += tuple[a];
				else
					transform += tuple[a] + ",";
			}
			
			if(offset.equals("0")){
				
				_firstLine =true;
			}
			
			if(!_firstLine){
/**RepairConstant*
				for(int cfdcount = 0; cfdcount < cfds.size(); cfdcount++){
					int ptsize = cfds.get(cfdcount).size()-2;
					ArrayList<String> cfd1 = cfds.get(cfdcount);
					String cfd = cfd1.get(0);
					int cfdLeftSize = cfd.split("@")[0].split(",").length;
					int cfdRightSize = cfd.split("@")[1].split(",").length;
					
					for(int ptcount = 0; ptcount < ptsize; ptcount++){
						boolean _match = true;
						int i = 0;
						while(_match && i < cfdLeftSize){
							String[] Index = cfd1.get(cfd1.size()-1).split(",");
							_match = _match && match(cfd1.get(ptcount+1).split("&")[0].split(",")[i], tuple[Integer.parseInt(Index[i])]);
							i++;
						}
						
						if(_match){
							for(i = 0; i < cfdRightSize; i++){
								if( !cfd1.get(ptcount+1).split("&")[1].split(",")[i].equals("_") ){
									
									String ll=cfd1.get(ptcount+1).split("&")[1].split(",")[i];
									int index=Integer.parseInt(cfd1.get(cfd1.size()-1).split(",")[cfdLeftSize+i]);
									if( !match(ll,tuple[index])){
										
										String fixed = "" ;
										tuple[index] = ll;
										for(int i1 = 0; i1 < tuple.length; i1++){
											if(i1 != tuple.length - 1)
												fixed += tuple[i1] + ",";
											else
												fixed += tuple[i1];
										}
										transform = fixed;
									}
								}
							}
						}
					}
				}
/**group*/		
				for(int cfdcount = 0; cfdcount < cfds.size(); cfdcount++){
					int ptsize = cfds.get(cfdcount).size()-2;
					ArrayList<String> cfd1 = cfds.get(cfdcount);
					String cfd = cfd1.get(0);
					
					
					int cfdLeftSize = cfd.split("@")[0].split(",").length;
					int cfdRightSize = cfd.split("@")[1].split(",").length;
					Integer cfd_index = new Integer(cfdcount);
					
					
					for(int ptcount = 0; ptcount < ptsize; ptcount++){
						
						boolean _match = true;
						int i = 0;
						
						
						
						while(_match && i < cfdLeftSize){
							_match = _match && match(cfd1.get(ptcount+1).split("&")[0].split(",")[i], tuple[Integer.parseInt(cfd1.get(cfd1.size()-1).split(",")[i])]);
							i++;
						}
						
						if(_match){
							
							
							String left = new String();
							for(i = 0; i < cfdLeftSize; i++){
								if(i == cfdLeftSize - 1)
									left += tuple[Integer.parseInt(cfd1.get(cfd1.size()-1).split(",")[i])];
								else
									left += tuple[Integer.parseInt(cfd1.get(cfd1.size()-1).split(",")[i])] + "\t";
							}
							
							for(i = 0; i < cfdRightSize; i++){
								
								Integer attr_index = Integer.parseInt(cfd1.get(cfd1.size()-1).split(",")[cfdLeftSize+i]);
								
								if( cfd1.get(ptcount+1).split("&")[1].split(",")[i].equals("_") ){
									_appear = true;
										
										String info = cfd_index.toString() + "," + left + "," + attr_index.toString() + ",0";
										String x = offset + "&" + transform;
										thisValue.set(x);
										thisKey.set(info);
										context.write(thisKey, thisValue);
										
								}
							}
						}
					}
				}
			}			
			
			if(!_appear){				
				String info = "0,0,0,1";
				thisKey.set(info);				
				String x = offset + "&" + transform;
				thisValue.set(x);
				context.write(thisKey, thisValue);				
				
			}
		}
	}


	public static class Var_GroupReducer extends Reducer<Text,Text,Text,Text> { 
		
		private Text thisValue = new Text();
		private Text thiskey = new Text(); 

		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			
			
			String[] total = key.toString().split(",");
		    String cfd_index = total[0];
			String attrIndex = total[2];
			int attr_index = Integer.parseInt(attrIndex);
			String tag = total[3];
			
			
			if(tag.equals("1")){
				for (Text val_iter : values) {
					thiskey.set(val_iter.toString());
					thisValue.set("1,0,0,0");
					context.write(thiskey, thisValue);
					
				}								
			}			
			else{
				
				Vector<String> possible = new Vector<String>();
				Vector<Integer> count = new Vector<Integer>();
				Vector<String> vector = new Vector<String>();
				
				
				
				for (Text val_iter : values) {	
					
					vector.add(val_iter.toString());
					
					String[] info = val_iter.toString().split("&");
					String[] tuple = info[1].split(",");
					if( !possible.contains(tuple[attr_index]) ) {
						possible.add(tuple[attr_index]);
						count.add(1);
					}
					else{
						int i = possible.indexOf(tuple[attr_index]);
						int temp = count.get(i);
						count.set(i, temp + 1);
					}																
				}
				
				if(possible.size() > 1){
					
					
					
					int temp = 0;
					int max_index = 0;
					for(int max = 0; max < count.size(); max++){
						if(temp < count.get(max)){
							temp = count.get(max);
							max_index = max;
						}
					}
					String fixValue = possible.get(max_index);
					
					
					
					for (String str : vector) {
						thiskey.set(str);
						String solution = "0," + attrIndex + "," + fixValue + "," + cfd_index;
						thisValue.set(solution);
						context.write(thiskey, thisValue);
					}
								
				}
				else{
					for (String str : vector) {
						thiskey.set(str);
						thisValue.set("1,0,0,0");
						context.write(thiskey, thisValue);
						
					}
				}				
			}						
		}
	}


	public static boolean match(String x, String y){
	
		if(x.equals("_") || y.equals("_"))		
			return true;	
		if(x.equals(y))		
			return true;	
		else		
			return false;
	}
	
	public int run(String args[]) throws Exception{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[1]), true);
		
		getConf().addResource(new Path(args[2]));
		String cfd1 = getConf().get("in.cfd","/home/ning/桌面/input/cfd.txt");
		DistributedCache.addCacheFile(new Path(cfd1).toUri(),conf);
		
		Job job = new Job(conf, "Variable group");   
		job.setJarByClass(RepConstant_Group.class);	    
	    job.setMapperClass(Var_GroupMapper.class);
	    job.setReducerClass(Var_GroupReducer.class);
	    
		job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
		int reduce_task_number = conf.getInt("num.reduceTask", 2);
		job.setNumReduceTasks(reduce_task_number);
	    
	    
		FileInputFormat.addInputPath(job, new Path(args[0]));	    
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    int res = job.waitForCompletion(true) ? 0 : 1;
		if (res == 0) {
			System.out.println("VariableGroup done.");
		} else {
			System.out.println("VariableGroup failed.");
		}

		return res;
	}
	public static void main(String[] args) throws Exception {
		String[] path= new String[3];
		path[0]="out_RepairUnconsist/PreProcess/part-r-00000";
		path[1]="output";
		path[2]="/home/ning/桌面/configuration.xml";
		int res = ToolRunner.run(new Configuration(), new RepConstant_Group(), path);
		System.exit(res);
	}
}
