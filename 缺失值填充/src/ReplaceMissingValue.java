import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
public class ReplaceMissingValue extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception{
		long start = System.currentTimeMillis();
		String[] path = new String[2];

			System.out.println("useage: *.xml inputfile ");
			path[0] = args[1];
			path[1] = args[0];






		
		int res = ToolRunner.run(new Configuration(),new ReplaceMissingValue(),path);
		System.out.println("Use time : "+(System.currentTimeMillis()-start)/1000.0 + " s");
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		getConf().addResource(new Path(args[0]));
		System.out.println(getConf());
		
		
		String workdir = getConf().get("workdir","ReplaceMissingValue/");
		System.out.println(workdir);
		FileSystem fs = FileSystem.get(getConf());
		if (fs.exists(new Path(workdir)))
			fs.delete(new Path(workdir),true);
		if (fs.exists(new Path("PostProcess")))
			fs.delete(new Path("PostProcess"),true);
		fs.mkdirs(new Path(workdir));
		String path[] = new String[4];
		String method =getConf().get("method","classify");
		int res = 0; 
		if(method.matches("classify")){
			path[0] = args[0];
			path[1] = args[1];
			path[2] = workdir+"output1";
			path[3] = workdir+"output";
			res = ToolRunner.run(getConf(),new NaiveBayesI(),path);
			if(res != 0)
				return res;
			res = ToolRunner.run(getConf(), new NaiveBayesII(),path);
			System.out.println("1");
		}else{
			
		}
		fs.copyToLocalFile(new Path("PostProcess"), new Path(workdir));


		System.out.println("Job Done!");
		return res;
	}
}
