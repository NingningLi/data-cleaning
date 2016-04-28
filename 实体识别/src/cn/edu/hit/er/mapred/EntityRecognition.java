package cn.edu.hit.er.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import cn.edu.hit.er.util.DataProcess;
import cn.edu.hit.er.util.Main;

/**
 * The Main class of ICrawler
 * 
 * @author hr1
 */
public class EntityRecognition extends Configured implements Tool {

	public static Logger logger = Logger.getLogger(EntityRecognition.class);

	public static void main(String[] args) throws Exception {

		long start = System.currentTimeMillis();
		if (2 != args.length) {
			System.out.println("useage: inputfile *.xml");
		}
		/**
		 * args[0]:cn.edu.hit.er.mapred.EntityRecognition
		 * args[1]：111
		 * args[2]：configuration.xml
		 */
		
		int res = ToolRunner.run(new Configuration(), new EntityRecognition(),
				args);
		
		System.out.println("Use time : " + (System.currentTimeMillis() - start)/1000 + " s" );
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		
		
		getConf().addResource(new Path(args[2]));
		
		System.out.println(getConf());

		
		String workdir = getConf().get("org.er.workdir", "EntityRecognition/");
		System.out.println(workdir);
		

		int res = 0;
		
		
		FileSystem fs = FileSystem.get(getConf());
		if (!fs.exists(new Path(workdir)))
			fs.mkdirs(new Path(workdir));
		else 
			fs.delete(new Path(workdir),true);

		
		
		fs.copyFromLocalFile(new Path(args[1]), new Path("input/origin"));
					
		
		
			
			System.out.println("data PreProcess..");
			String[] path=new String[2];
			path[0]="input/origin";
			path[1]=workdir + "input/";
			DataProcess.execute(path);
				
			System.out.println("PreMapReduce...");
			
			path[0] = path[1];
			path[1] = workdir + "The_Property_in";

			
			res = ToolRunner.run(getConf(), new PreMR(), path);
			if (res != 0)
				return res;

			
			System.out.println("MapReduce1...");
			path[0] = path[1];
			path[1] = workdir + "The_property_out1";

			res = ToolRunner.run(getConf(), new MR1Driver(), path);
			if (res != 0)
				return res;

			
			System.out.println("MapReduce2...");

			path[0] = path[1];
			path[1] = workdir + "The_property_out2";

			res = ToolRunner.run(getConf(), new MR2Driver(), path);
			if (res != 0) {
				return res;
			}
			
			System.out.println("MapReduce3...");
			path[0] = path[1];
			path[1] = workdir + "The_property_out3";
			res = ToolRunner.run(getConf(), new MR3Driver(), path);
			if (res != 0)
				return res;	
			
		System.out.println("GatherMR...");
		path[0] = path[1];
		path[1] = workdir + "res";
		res = ToolRunner.run(getConf(), new GatherMR(), path);
		if (res != 0)
			return res;		
		
		System.out.println("calculated finished\n");
		
		
		
		fs.copyToLocalFile(new Path(path[1]), new Path("out"));
		
		fs.copyToLocalFile(new Path(workdir),new Path(workdir));
		fs.delete(new Path(workdir),true);
		
		Main.getResult("out", "final_res","final_pro");

		return res;

	}
}
