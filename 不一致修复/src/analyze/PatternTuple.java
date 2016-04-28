

package analyze;

import java.util.*;

public class PatternTuple {

	 Vector<String> LeftValue = new Vector<String>();
	 Vector<String> RightValue = new Vector<String>();
	 
	 public String getPtLvalue(int index){
		 return LeftValue.get(index);
	 }

	 public String getPtRvalue(int index){
		 return RightValue.get(index);
	 }
}
