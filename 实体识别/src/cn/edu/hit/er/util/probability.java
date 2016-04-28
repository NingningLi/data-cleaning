package cn.edu.hit.er.util;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;


public class probability {
	
	public static double getProbability(String name,int ith,String configuration) throws IOException{
		double probability = 0f;
		int line=ith;
		Configuration conf=new Configuration();
		conf.addResource(new Path(configuration));
		String probDir = "/usr/qingxi/probabilitydir/";
			
		File f=new File(probDir+"p");
		BufferedReader br=new BufferedReader(new FileReader(f));	
		
		
		int row=Integer.parseInt(name);
			
		for(int i=0;i<row-1;i++){
			br.readLine();
		}
		String s=br.readLine();
		probability=Double.parseDouble(  s.split("\\|")[line-1]);
			
		
		br.close();
		return probability;
	}
}
