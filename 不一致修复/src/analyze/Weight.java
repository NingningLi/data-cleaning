package analyze;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Vector;

public class Weight {

	Vector<Double> weight = new Vector<Double>();
	public Weight (){
		
	}
	public Weight(String Path){
		try {			         
			 File file=new File(Path);
			          			          
			 if(file.isFile() && file.exists()){ 
				 InputStreamReader read = new InputStreamReader(new FileInputStream(file));			          		    			
				 BufferedReader bufferedReader = new BufferedReader(read);			          		    
				 String lineTxt = bufferedReader.readLine();			          		    
				 String[] value = lineTxt.split("\t");			           
		    
				 for (String i : value) {			        	 		    			    	
					 weight.add(Double.parseDouble(i));					
					 
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
	
	public double getWeight(int i){
		return weight.get(i);
	}
}
