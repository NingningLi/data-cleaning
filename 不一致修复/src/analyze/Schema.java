

package analyze;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.*;

public class Schema{

	Vector<String> index = new Vector<String>();
	
	public Schema(String Path){
		
		 try {			         
			 File file=new File(Path);
			          			          
			 if(file.isFile() && file.exists()){ 
				 InputStreamReader read = new InputStreamReader(new FileInputStream(file));			          		    			
				 BufferedReader bufferedReader = new BufferedReader(read);			          		    
				 String lineTxt = bufferedReader.readLine();			          		    
				 String[] attr = lineTxt.split("\t");			           
		    
				 for (String i : attr) {			        	 		    			    	
					 index.add(i);						
					 
				 }			           		     
				 read.close();			  			
			 }			 
			 else{			  
				 System.out.println("找不到指定的文件");			 
			 }			          			  				 
		 } catch (Exception e) {			   
			 System.out.println("读取文件内容出错");			   
			 e.printStackTrace();			 		
		 }
	}	
}
