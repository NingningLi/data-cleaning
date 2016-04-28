import java.util.*;



public class RelatedInformation {
	public boolean flag = false;
	public Set<String> none_tuple ;
	public Set<String> have_tuple ;
	public RelatedInformation(){
		this.flag=false;
		this.none_tuple = new HashSet<String>();
		this.have_tuple = new HashSet<String>();
	}
}
