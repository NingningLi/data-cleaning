

package analyze;

import java.util.*;

public class cfd {

	/**
	 * @param args
	 */
	Vector<String> LeftAttr = new Vector<String>();
	Vector<String> RightAttr = new Vector<String>();
	
	Vector<Integer> LeftIndex = new Vector<Integer>();
	Vector<Integer> RightIndex = new Vector<Integer>();
	
	Vector<PatternTuple> PTuple = new Vector<PatternTuple>();
	
	public int getPtSize(){
		return PTuple.size();
	}
	
	public int getCfdLSize(){
		return LeftAttr.size();
	}
	
	public int getCfdRSize(){
		return RightAttr.size();
	}
	
	public PatternTuple getpt(int i){
		return PTuple.get(i);
	}
	
	public int getCfdLindex(int i){
		return LeftIndex.get(i);
	}
	
	public int getCfdRindex(int i){
		return RightIndex.get(i);
	}

}
