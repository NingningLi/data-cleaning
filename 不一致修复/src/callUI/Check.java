package callUI;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class Check extends Configured implements Tool{

	public static class VarPartOneMapper extends Mapper<Object, Text, Text, Text>{
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
		@Override
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {	
			
			int locate = value.toString().indexOf('#');			
			String offset = value.toString().substring(0, locate);
			String line = value.toString().substring(locate + 1);
			String[] tuple = line.split("\t");
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
								
								if( !cfd1.get(ptcount+1).split("&")[1].split(",")[i].equals("_") ){
									
									String ll=cfd1.get(ptcount+1).split("&")[1].split(",")[i];
									attr_index=Integer.parseInt(cfd1.get(cfd1.size()-1).split(",")[cfdLeftSize+i]);
									if( !match(ll,tuple[attr_index])){
										
										context.write(new Text("0,0,0,1"), new Text("true"));
									}
								}
								
								if( cfd1.get(ptcount+1).split("&")[1].split(",")[i].equals("_") ){
									
										
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
		}
	}

	public static class VarPartOneReducer extends Reducer<Text,Text,NullWritable,Text> { 
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			
			String[] total = key.toString().split(",");
			String attrIndex = total[2];
			int attr_index = Integer.parseInt(attrIndex);
			String tag=total[3];
			if(tag.equals("1")){
				context.write(NullWritable.get(), new Text("true"));;
			}else{
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
					
					context.write(NullWritable.get(), new Text("true"));;
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

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		
		getConf().addResource(new Path(args[2]));
		String cfd1 = getConf().get("in.cfd","input/cfd.txt");
		DistributedCache.addCacheFile(new Path(cfd1).toUri(),conf);
		
		Job job = new Job(conf, "Check Variable Violate Exist or not ");   
		job.setJarByClass(Check.class);	    
	    job.setMapperClass(VarPartOneMapper.class);
	    job.setReducerClass(VarPartOneReducer.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
		int reduce_task_number = conf.getInt("num.reduceTask", 2);
		job.setNumReduceTasks(reduce_task_number);
		
	    FileInputFormat.addInputPath(job, new Path(args[0]));	    
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    int res = job.waitForCompletion(true) ? 0 : 1;
		if (res == 0) {
			System.out.println("CheckVar done.");
		} else {
			System.out.println("CheckVar failed.");
		}
		return res;
	}
}
