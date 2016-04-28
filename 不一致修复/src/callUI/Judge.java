package callUI;

import java.io.ByteArrayOutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
 
/** 
 * 
 * @created May 26, 2012 12:28:49 PM 
 *  
 */ 
public class Judge { 
 
    /** 
     * @param args 
     */ 
    static boolean judge(String args) throws Exception{ 
    	
        String uri = args; 
        
        Configuration conf = new Configuration(); 
        conf.set("Hadoop.job.ugi", "hadoop-user,hadoop-user"); 
         
        
        FileSystem fs = FileSystem.get(URI.create(uri),conf); 
        FSDataInputStream in = null; 
        try{ 
            
            in = fs.open( new Path(uri) ); 
            
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            IOUtils.copyBytes(in, outputStream,50,false);   
            String i=outputStream.toString().trim();
            
            if(i.equals("true")){
            	return true;
            }
            else{
            	return false;
            }
            
        }finally{ 
            IOUtils.closeStream(in); 
        } 
    } 
    public static void main(String[] args) throws Exception{ 
    	boolean res;
    	res = judge("hdfs:
    	System.out.println("res:	"+res);
    }
 
} 
