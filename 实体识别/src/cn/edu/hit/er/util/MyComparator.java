package cn.edu.hit.er.util;

import java.util.Comparator;


public class MyComparator implements Comparator<String> {

	@Override
	public int compare(String o1, String o2) {
		
		
		return Integer.parseInt(o1) - Integer.parseInt(o2);
	}

}
