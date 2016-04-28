package repair;

import java.util.HashMap;
import java.util.Vector;


public class Resolved {

	public static HashMap<String, String> ResolvedMap = new HashMap<String, String>();
	public static HashMap<String, Double> PossibleMap = new HashMap<String, Double>();
	public static Vector<String> TupleIndex = new Vector<String>();
	public static Vector<Double> TupleProbability = new Vector<Double>();
	public static double p = 1.0; 
	public static void main(String[] args) {
		double x = (double)1/2;
		System.out.println(x);

	}
}
