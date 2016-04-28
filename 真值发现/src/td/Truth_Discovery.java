package td;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Truth_Discovery extends Configured implements Tool {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		long start =System.currentTimeMillis();
		
		String[] path = new String[2];
		if(args.length == 0){
			System.out.println("Please type the ConfigurationFileName and InputFileName!\nNow we are using the defult configuration");
			path[0] = "configuration.xml";
			path[1] = "input/input.txt";
		}else{
			path = args;
		}
		int res = ToolRunner.run(new Configuration(), new Truth_Discovery(),path);
		System.out.println("Use time : " + (System.currentTimeMillis() - start)/1000 + " s" );
		System.exit(res);
	}
	public int run(String[] args) throws Exception{
		getConf().addResource(new Path(args[0]));
		System.out.println(getConf());
		
		
		String workdir = getConf().get("workdir","TruthDiscovery/");
		System.out.println(workdir);
		FileSystem fs = FileSystem.get(getConf());
		if (fs.exists(new Path(workdir)))
			fs.delete(new Path(workdir),true);
		fs.mkdirs(new Path(workdir));
		String path[] = new String[3];
		int res = 0; 
		path[0] = args[0];
		path[1] = args[1];
		path[2] = workdir+"output";
		res = ToolRunner.run(getConf(),new truthDiscovery(),path);
		if(res != 0)
			return res;
		fs.copyToLocalFile(new Path(workdir), new Path(workdir));
		fs.delete(new Path(workdir), true);
		fs.delete(new Path("input"),true);
		System.out.println("Job Done!");
		return res;
	}

}
