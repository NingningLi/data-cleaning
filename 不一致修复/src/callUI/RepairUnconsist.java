package callUI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import process.PreProcess;
import repair.RepairVariable;
import repair.Resolved;
import repair.RepConstant_Group;
import analyze.Schema;
import analyze.cfds;


public class RepairUnconsist extends Configured implements Tool {
	
	public static boolean Dirty = false;
	
	public static void main(String[] args) throws Exception {
		long start = System.currentTimeMillis();
		String[] path = new String[3];
		if (3 == args.length) {
			System.out.println("useage: *.xml inputfile ");
			path[0] = args[0];
			path[1] = args[1];
			path[2] = args[2];
		}
		if (args.length==2){
			path[0] = "callUI.RepairUnconsist";
			path[1] = "/home/ning/桌面/configuration.xml";
			path[2] = "/home/ning/桌面/input";
		}

		
		int res = ToolRunner.run(new Configuration(), new RepairUnconsist(),
				path);
		
		System.out.println("Use time : " + (System.currentTimeMillis() - start)/1000 + " s" );
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		getConf().addResource(new Path(args[1]));
		System.out.println(getConf());

		
		String workdir = getConf().get("workdir", "RepairUnconsist/");
		System.out.println(workdir);
		
		FileSystem fs = FileSystem.get(getConf());
		if (!fs.exists(new Path(workdir)))
			fs.mkdirs(new Path(workdir));
		else 
			fs.delete(new Path(workdir),true);
		
		if (fs.exists(new Path("input")))
			fs.delete(new Path("input"),true);
		fs.copyFromLocalFile(new Path(args[2]), new Path("input"));
				
		String intxt = getConf().get("in.txt","input/f0.txt");
		String incfd = getConf().get("in.cfd","input/cfd.txt");
		
		cfds cfdRules = new cfds(incfd,new Schema(intxt));
		System.out.println("cfdRules has  " + cfdRules.getCfdSize(cfdRules) + "  cfd");
		cfdRules.cfdsPrint();
		boolean CfdConsist = cfdRules.cfdCheck();
		System.out.println("\n this cfd rules is true?");
        System.out.println(CfdConsist);

        boolean over = false;
        int res = 0;
        if(CfdConsist){        	
        	String path[] = new String[3];
        	System.out.println("\nPreProcess..");
        	path[0] = intxt;
        	path[1] = workdir+"PreProcess";
        	PreProcess.execute(path[0],path[1]);
			
			
        	System.out.println("\nRepair..");        	
			path[0] = path[1];
        	path[1] = workdir+"output/VarGroup";
        	path[2] = args[1];
        	res = ToolRunner.run(getConf(), new RepConstant_Group(), path);
			if (res != 0)
				return res;
			
			path[0] = path[1];
        	path[1] = workdir+"output/final";
        	res = ToolRunner.run(getConf(), new RepairVariable(), path);
			if (res != 0)
				return res;
        	int i = 0;
			while((over == false)&&(i<3)){
				System.out.println("\nThis is the "+(i+1)+"th	"+"time to check and repair!");
				System.out.println("\nCheck..");
				path[0] = workdir+"output/final";
	        	path[1] = workdir+"Check";
	        	res = ToolRunner.run(getConf(), new Check(), path);
				if (res != 0)
					return res;
				Dirty = Judge.judge(workdir+"Check/part-r-00000");
        		System.out.println("Dirty ?\t" + RepairUnconsist.Dirty );
            	if( Dirty )
            	{
            		path[0] = "output/final";
                	path[1] = workdir+"output/VarGroup";
                	res = ToolRunner.run(getConf(), new RepConstant_Group(), path);
        			if (res != 0)
        				return res;
        			
        			path[0] = path[1];
                	path[1] = workdir+"output/final";
                	res = ToolRunner.run(getConf(), new RepairVariable(), path);
        			if (res != 0)
        				return res;
            		Dirty = false;
                	i++;
            		continue;
            	}
            	 if(!Dirty)
                 {
                 	over = true;
                 	System.out.println("over :" + over);
                 }
        	}
        	System.out.println("\nPostProcess..");
        	String PostProcess = workdir+"PostProcess";
        	process.PostProcess.execute(workdir+"output/final", PostProcess);
        }
        else{
        	System.out.println("Cfd rules are not consist, please check !");
        }
        
      System.out.println("Finish !");
      
      fs.copyToLocalFile(new Path(workdir), new Path("out_RepairUnconsist"));
      fs.delete(new Path(workdir), true);
      fs.delete(new Path("input"),true);
       double pr = Resolved.p;
       System.out.println("This repaired result's consist probability is : " + pr);
		return res;
	}
}
