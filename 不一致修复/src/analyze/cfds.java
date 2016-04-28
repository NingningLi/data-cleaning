

package analyze;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.*;

public class cfds {

    private Vector<cfd> rules = new Vector<cfd>();
	public cfds() {
	}
	public cfds(String Path, Schema schema){
		
		 try {
	          File file=new File(Path);
	          
	          if(file.isFile() && file.exists()){ 
	        	  
	           InputStreamReader read = new InputStreamReader(new FileInputStream(file));
	           BufferedReader bufferedReader = new BufferedReader(read);
	           String lineTxt = null;
	           
	           while((lineTxt = bufferedReader.readLine()) != null){
	        	   
	        	   if(lineTxt.startsWith("%"))
	        		   break;
	        	   if(lineTxt.startsWith("$")){
	        		   cfd cfd1 = new cfd();
	        		   lineTxt = lineTxt.substring(1);
	        		   String[] TokenPaser = lineTxt.split("#");
	        		   String[] cfdAttr = TokenPaser[0].split("@");
	        		   String[] leftAttr = cfdAttr[0].split(",");
	        		   String[] rightAttr = cfdAttr[1].split(",");
	        		   
	        		   for (String i : leftAttr) {
						   cfd1.LeftAttr.add(i);
						   cfd1.LeftIndex.add(schema.index.indexOf(i));						
					}
	        		   for (String i : rightAttr) {
						   cfd1.RightAttr.add(i);
						   cfd1.RightIndex.add(schema.index.indexOf(i));
					}
	        		   
	        		   for(int counter = 1; counter < TokenPaser.length-1; counter ++){
	        			   String[] ptValue = TokenPaser[counter].split("&");
	        			   String[] leftValue = ptValue[0].split(",");     			   
	        			   String[] rightValue = ptValue[1].split(",");
	        			   PatternTuple tuple = new PatternTuple();
	        			   
	        			   for (String i : leftValue) {
							tuple.LeftValue.add(i);
						}
	        			   for (String i : rightValue) {
							tuple.RightValue.add(i);
						}
	        			   
	        			   cfd1.PTuple.add(tuple);
	        		   }
	        		   
	        		   rules.add(cfd1);
	        	   }
	        		   
	           }
	           			           
	           read.close();
	  }else{
	   System.out.println("找不到指定的文件");
	  }
	  } catch (Exception e) {
	   System.out.println("读取文件内容出错");
	   e.printStackTrace();
	  }
		
	}
	
	public void cfdsPrint(){
		int counter = 1;
		for (cfd i : rules) {
			System.out.println("-----  cfd" + counter + "  is :-----\n");
			
			System.out.print("left:");
			for (String str : i.LeftAttr) {
				System.out.print("\t" + str);
			}
			System.out.print("\t right:");
			for (String str : i.RightAttr) {
				System.out.print("\t" + str);
			}
			System.out.print("\n");
			
			for (PatternTuple pt : i.PTuple) {
				for (String str : pt.LeftValue) {
					System.out.print("\t" + str);
				}
                for (String str : pt.RightValue) {
                	System.out.print("\t" + str);
				}
                System.out.print("\n");
			}
						
			counter ++;
		}
	}
	
	public boolean cfdCheck(){
		boolean _all = true;
		HashMap<String, String> map = new HashMap<String, String>();
		
		for (cfd cfd_iter : rules) {
			
			for (PatternTuple pt_iter : cfd_iter.PTuple) {
				
				for (String lv_iter : pt_iter.LeftValue) {
					
					if( !lv_iter.equals("_") ){
						_all = false;
						break;
					}
				}
				
				
				
					
				if(_all){
						int ra_size = cfd_iter.RightAttr.size();
						for(int i = 0; i < ra_size; i++){
							
							String key = cfd_iter.RightAttr.get(i);
							String value = pt_iter.RightValue.get(i);
							
							if( !value.equals("_") ){
								if( map.containsKey(key) && !map.get(key).equals(value) ){
									return false;
								}
								map.put(key, value);
								
								
								System.out.println("(key, value) = " + "( "+ key + ", " + value +" )");
							}
						}
					}
				_all = true;	
				
			}
		}
		
		if(map.isEmpty())
			return true;
		int oldSize = map.size();
		int newSize = oldSize;
		
		while(true){
			for (cfd cfd_iter : rules) {				
				int la_size = cfd_iter.LeftAttr.size();
				
				for (PatternTuple pt_iter : cfd_iter.PTuple) {
					
					boolean _find = false;
					for (int i = 0; i < la_size; i++) {
						
						String key = cfd_iter.LeftAttr.get(i);
						String value = pt_iter.LeftValue.get(i);
						if(!value.equals("_")){
							if( map.containsKey(key) && map.get(key).equals(value) ){
								_find = true;
								break;
							}
						}
					}
					
					if(_find){
						int ra_size = cfd_iter.RightAttr.size();
						for(int i = 0; i < ra_size; i++){
							String key = cfd_iter.RightAttr.get(i);
							String value = pt_iter.RightValue.get(i);
							if(!value.equals("_")){
								if(map.containsKey(key) && !map.get(key).equals(value)){
									
									
									
									return false;
								}
								map.put(key, value);
								
								
								System.out.println("(key, value) = " + "( "+ key + ", " + value +" )");
							}
						}
					}
				}
			}
			
			newSize = map.size();
			if(oldSize != newSize)
				oldSize = newSize;
			else
				return true;			
		}
		
	}
	
	public int getCfdSize(cfds cfds1){
		return cfds1.rules.size();
	}
	
	public cfd getcfd( int i){
		return rules.get(i);
	}
}
