package cn.edu.hit.er.util;

import java.util.*;
import java.util.Map.Entry;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

class vexNode 
{
	int type;
	int simNum;
	ArrayList<Integer> adjold = new ArrayList<Integer>();
	ArrayList<Integer> contain = new ArrayList<Integer>();;
	Map<Integer,Double> probability = new TreeMap<Integer,Double>();
	
	vexNode() {
		type = 0;
		simNum = 0;
		adjold.clear();
		contain.clear();
	}
};

class Node { 
	int anc = 0;
	int mark = 0;
	int deep = 0;
	Set<Integer> adj = new TreeSet<Integer>();
	Map<Integer,Double> probability = new TreeMap<Integer,Double>();
	
	Node() {
		adj.clear();
		anc = 0;
		mark = 0;
		deep = 0;
	}
};

public class Main {

	
	static int ORIGIN = 1;
	static int AGGRES = 2;
	static int NEWNUL = 3;
	static int OLDNUL = 4;
	static double lamta;
	static Configuration config = new Configuration();
	
	static InputStream in = null;
	
	static {
			config.addResource(new Path("configuration.xml"));
			
			lamta = Double.parseDouble(config.get("org.er.lamta"));
			
			
	}

	static Map<Long, Boolean> Edge = new TreeMap<Long, Boolean>();
	static Map<Integer, vexNode> Graph = new TreeMap<Integer, vexNode>();
	static int setnumber = 0;
	static ArrayList<Node> nodes = new ArrayList<Node>();
	static Map<Long, Boolean> bridge = new TreeMap<Long, Boolean>();
	static ArrayList<Set<Integer>> subgraphs = new ArrayList<Set<Integer>>();
	/* Notice:: Data and Result must be NOT under the same directory */
	static String simpairPath = "out";
	static String resultPath = "result.txt";
	static String proPath = "probability.txt";

	static long hash(int a, int b) {
		long aa, bb;
		aa = a;
		bb = b;
		return ((aa << 32) | bb);
	}

	static void print_graph() {
		for (int i = 1; i < nodes.size(); i++) {
			Iterator<Integer> it1 = (nodes.get(i)).adj.iterator();
			System.out.print(i + " : ");
			while (it1.hasNext()) {
				System.out.print(it1.next() + " ");
			}
			System.out.print("\n");
		}
	}

	static void build_graph(String pathname) throws Exception {
		
		System.out.println("Start to Open File Directory. . .");
		String filename = new String();
		File directory = new File(pathname);
		File flist[] = directory.listFiles();
		nodes.clear();
		for (int count = 0; count < flist.length; count++) {
			if (flist[count].isFile() && flist[count].getName().startsWith("part")) {
				filename = flist[count].getAbsolutePath();
				System.out.println("Start to Read File : " + filename);
				/* read in this file */
				Reader r = new BufferedReader(new FileReader(filename));
				StreamTokenizer stok = new StreamTokenizer(r);
				stok.parseNumbers();
				stok.nextToken();
				int a, b;
				double pro;
				while (stok.ttype != StreamTokenizer.TT_EOF) {
					
					a = (int) stok.nval;
					stok.nextToken();
					b = (int) stok.nval;
					stok.nextToken();
					pro = (double)stok.nval;
					stok.nextToken();
					a++;
					b++;
					if (nodes.size() < a && a >= b) {
						nodes.ensureCapacity(a);
						for (int i = nodes.size(); i < a; i++) {
							Node tmp1 = new Node();
							nodes.add(tmp1);
						}
					} else if (nodes.size() < b && b >= a) {
						nodes.ensureCapacity(b);
						for (int i = nodes.size(); i < b; i++) {
							Node tmp2 = new Node();
							nodes.add(tmp2);
						}
					}

					a--;
					b--;
					
					nodes.get(a).adj.add(b);
					nodes.get(b).adj.add(a);
					nodes.get(a).probability.put(b, pro);
					nodes.get(b).probability.put(a, pro);
				}
				r.close();
			}
		}

		System.out.println("Build Graph Finished!");

	}

	static void dfs(int i, int father) {
		
		(nodes.get(i)).mark = -1;
		(subgraphs.get(subgraphs.size() - 1)).add(i);
		Iterator<Integer> it1 = (nodes.get(i)).adj.iterator();
		while (it1.hasNext()) {
			int j = (int) it1.next();
			if (j != father && (nodes.get(j)).mark != -1) {
				dfs(j, father);
			}
		}
	}

	static void DFS(int i, int father, int deep) {
		
		int sons = 0;
		(nodes.get(i)).mark = 1;
		(nodes.get(i)).deep = ((Node) nodes.get(i)).anc = deep;
		Iterator<Integer> it1 = (nodes.get(i)).adj.iterator();
		while (it1.hasNext()) {
			int j = (int) it1.next();
			if (j != father && ((Node) nodes.get(j)).mark == 1) {
				if (((Node) nodes.get(j)).deep < ((Node) nodes.get(i)).anc) {
					((Node) nodes.get(i)).anc = ((Node) nodes.get(j)).deep;
				}
			}
			if (((Node) nodes.get(j)).mark == 0) {
				DFS(j, i, deep + 1);
				sons++;
				if (((Node) nodes.get(j)).anc < ((Node) nodes.get(i)).anc) {
					((Node) nodes.get(i)).anc = ((Node) nodes.get(j)).anc;
				}

				if ((father == -1 && sons > 1)
						|| (father != -1 && ((Node) nodes.get(i)).deep < ((Node) nodes
								.get(j)).anc)) {

					System.out.println("Cut Bridge :  " + i + " --  " + j);
					bridge.put(hash(j, i), true);
					
					((Node) nodes.get(j)).adj.remove(i);
					it1.remove();

					Set<Integer> tmp = new TreeSet<Integer>();
					tmp.clear();
					subgraphs.add(tmp);
					dfs(j, i);
				}
			}
		}
		((Node) nodes.get(i)).mark = 2;
	}

	static void cut_bridge() {
		System.out.println("--------------------");
		System.out.println("Start to Cut Bridge . . . ");
		bridge.clear();
		for (int i = 1; i < nodes.size(); i++) {
			if (((Node) nodes.get(i)).mark == 0) {
				DFS(i, -1, 0);
				if (((Node) nodes.get(i)).mark != -1) {
					Set<Integer> tmp = new TreeSet<Integer>();
					tmp.clear();
					subgraphs.add(tmp);
					dfs(i, -1);
				}
			}
		}
		System.out.println("Cut Bridge Finished!");
		System.out.println("--------------------");
	}

	
	static void LocalDivision() {
		int N = Graph.size();
		
		int AD = N;
		int BD = N + 1;
		
		Iterator<Entry<Long, Boolean>> iter = Edge.entrySet().iterator();
		int a, b;
		while (iter.hasNext()) {
			Map.Entry<Long, Boolean> entry = (Map.Entry<Long, Boolean>) iter
					.next();
			a = (int) (entry.getKey() >> 32);
			b = (int) ((entry.getKey()) & 0x00000000FFFFFFFF);
			(Graph.get(a)).adjold.add(b);
			(Graph.get(b)).adjold.add(a);
		}
		while (AD < BD) 
		{
			
			BD = AD;
			Iterator<Entry<Integer, vexNode>> top = Graph.entrySet().iterator();
			while (top.hasNext()) {
				Map.Entry<Integer, vexNode> entry = (Map.Entry<Integer, vexNode>) top
						.next();
				if (entry.getValue().type <= AGGRES)
				{
					a = entry.getKey();
					for (int c = 0; c < Graph.get(a).adjold.size(); c++)
					{
						b = Graph.get(a).adjold.get(c);
						if (b > a && Edge.containsKey(hash(a, b))
								&& Graph.get(b).type <= AGGRES) 
						{
							
							boolean update = false;
							
							
							Map<Integer, Integer> sapsb = new TreeMap<Integer, Integer>();
							sapsb.clear();
							for (int m = 0; m < Graph.get(a).contain.size(); m++) {
								sapsb.put(Graph.get(a).contain.get(m), 1);
							}
							for (int m = 0; m < Graph.get(b).contain.size(); m++) {
								sapsb.put(Graph.get(b).contain.get(m), 1);
							}
							
							
							boolean condition1 = true;
							if (Graph.get(a).adjold.size() < Graph.get(b).adjold
									.size()) 
							{
								for (int p1 = 0; p1 < Graph.get(a).adjold
										.size(); p1++) 
								{
									if (sapsb.containsKey(Graph.get(a).adjold
											.get(p1)) == false)
									{
										condition1 = false;
										break;
									}
								}
								if (condition1) {
									for (int p2 = 0; p2 < Graph.get(b).adjold
											.size(); p2++) 
									{
										if (sapsb
												.containsKey(Graph.get(b).adjold
														.get(p2)) == false)
										{
											condition1 = false;
											break;
										}
									}
								}
							}
							else {
								for (int p1 = 0; p1 < Graph.get(b).adjold
										.size(); p1++) 
								{
									if (sapsb.containsKey(Graph.get(b).adjold
											.get(p1)) == false)
									{
										condition1 = false;
										break;
									}
								}
								if (condition1) {
									for (int p2 = 0; p2 < Graph.get(a).adjold
											.size(); p2++) 
									{
										if (sapsb
												.containsKey(Graph.get(a).adjold
														.get(p2)) == false)
										{
											condition1 = false;
											break;
										}
									}
								}
							}

							update = condition1;

							
							
							
							int najnb = 0;
							
							Map<Integer, Integer> napb = new TreeMap<Integer, Integer>();
							napb.clear();
							if (Graph.get(a).adjold.size() > Graph.get(b).adjold
									.size()) {
								for (int m = 0; m < Graph.get(a).adjold.size(); m++) 
								{
									napb.put(Graph.get(a).adjold.get(m), 1);
								}
								for (int m = 0; m < Graph.get(b).adjold.size(); m++) 
								{
									if (napb.containsKey(Graph.get(b).adjold
											.get(m)))
									{
										najnb++;
									} else
									{
										napb.put(Graph.get(b).adjold.get(m), 1);
									}
								}
							}
							else {
								for (int m = 0; m < Graph.get(b).adjold.size(); m++) 
								{
									napb.put(Graph.get(b).adjold.get(m), 1);
								}
								for (int m = 0; m < Graph.get(a).adjold.size(); m++) 
								{
									if (napb.containsKey(Graph.get(a).adjold
											.get(m)))
									{
										najnb++;
									} else
									{
										napb.put(Graph.get(a).adjold.get(m), 1);
									}
								}
							}

							
							if (najnb > (double) lamta * (napb.size())) 
							{
								update = true;
							}

							if (update == true) {
								Graph.get(b).type = NEWNUL;
								Graph.get(a).type = AGGRES;

								
								Iterator<Entry<Integer, Integer>> tocon;
								for (int p = 0; p < Graph.get(b).contain.size(); p++) {
									Graph.get(a).contain
											.add(Graph.get(b).contain.get(p));
								}

								
								
								

								
								Iterator<Entry<Integer, vexNode>> ta;

								ta = Graph.entrySet().iterator();
								while (ta.hasNext()) {
									Map.Entry<Integer, vexNode> curentry = (Map.Entry<Integer, vexNode>) ta
											.next();
									if (curentry.getKey() > a) {
										Edge.remove(hash(a, curentry.getKey()));
										Edge.remove(hash(curentry.getKey(), a));
									}
									if (curentry.getKey() > b) {
										Edge.remove(hash(b, curentry.getKey()));
										Edge.remove(hash(curentry.getKey(), b));
									}
								}
								Graph.get(a).adjold.clear();
								tocon = napb.entrySet().iterator();
								while (tocon.hasNext()) {
									Map.Entry<Integer, Integer> curentry = (Map.Entry<Integer, Integer>) tocon
											.next();
									Graph.get(a).adjold.add(curentry.getKey());
									if (Graph.get(curentry.getKey()).type != OLDNUL
											&& curentry.getKey() != a
											&& sapsb.containsKey(curentry
													.getKey()) == false)
									{
										if (a < curentry.getKey()
												&& Edge.containsKey(hash(a,
														curentry.getKey())) == false)
										{
											Edge.put(
													hash(a, curentry.getKey()),
													true);
										} else if (a > curentry.getKey()
												&& Edge.containsKey(hash(
														curentry.getKey(), a)) == false) {
											Edge.put(
													hash(curentry.getKey(), a),
													true);
										}
									}
								}
								napb.clear();
								sapsb.clear();

								Graph.get(b).type = OLDNUL;
								Graph.get(b).adjold.clear();
								Graph.get(b).contain.clear();
								AD--;

							}

						}

					}
				}
			}
		}
	}

	static void subgraph(String pathname,String propath) throws IOException {

		System.out.println("Subset Result:   ");
		Iterator<Entry<Long, Boolean>> iter = bridge.entrySet().iterator();
		File f = new File(pathname);
		FileWriter fw = new FileWriter(f, false);
		File p = new File(propath);
		FileWriter pw = new FileWriter(p, false);		
		for (int i = 0; i < subgraphs.size(); i++) {
			Edge.clear();
			Graph.clear();
			
			Iterator<Integer> sub = subgraphs.get(i).iterator();
			if (subgraphs.get(i).size() != 1)
			{
				while (sub.hasNext()) {
					int tmpnode = sub.next();
					Iterator<Integer> it = nodes.get(tmpnode).adj.iterator();
					while (it.hasNext())
					{
						int tmpadj = it.next();
						if (tmpnode < tmpadj
								&& subgraphs.get(i).contains(tmpadj))
						{
							Edge.put(hash(tmpnode, tmpadj), true);
						}
					}

					
					vexNode newnode = new vexNode();
					newnode.type = ORIGIN;
					newnode.adjold.clear();
					newnode.adjold.add(tmpnode);
					newnode.contain.clear();
					newnode.contain.add(tmpnode);
					Graph.put(tmpnode, newnode);
				}

				
				LocalDivision();

				Iterator<Entry<Integer, vexNode>> outgraph = Graph.entrySet()
						.iterator();
				while (outgraph.hasNext())
				{
					Map.Entry<Integer, vexNode> entry = (Map.Entry<Integer, vexNode>) outgraph
							.next();
					if (entry.getValue().type == AGGRES&&entry.getValue().contain.size()>1)
					{
						
						System.out.print("  Subset  " + setnumber + " :  ");
						fw.write("Subset  " + setnumber + " :  ");
						double totalpro = 0;
						double tsize = 0;
						for (int k = 0; k < entry.getValue().contain.size(); k++) {
							
							int tp = entry.getValue().contain.get(k);
							System.out.print(tp + "  ");
							fw.write(tp + "  ");
							
							Iterator<Entry<Integer, Double>> proiter = nodes.get(tp).probability.entrySet().iterator();
							
							while(proiter.hasNext())
							{
								Map.Entry<Integer, Double> thispro = (Map.Entry<Integer, Double>)proiter.next();
								if(thispro.getKey()>tp&&entry.getValue().contain.contains(thispro.getKey()))
								{
									totalpro += thispro.getValue();
									tsize++;
								}
							}
						}
						totalpro = totalpro/tsize;
						System.out.print("\n");
						for (int k = 0; k < entry.getValue().contain.size(); k++)
						{
							int tp = entry.getValue().contain.get(k);
							System.out.print(" Probability "+tp+" : ");
							pw.write(" Probability "+tp+" : ");
							
							
							double thisnodepro = 0;
							Iterator<Entry<Integer, Double>> nodeproiter = nodes.get(tp).probability.entrySet().iterator();
							double asize = 0;
							while(nodeproiter.hasNext())
							{
								Map.Entry<Integer, Double> thisnodeentry = (Map.Entry<Integer, Double>)nodeproiter.next();
								if(entry.getValue().contain.contains(thisnodeentry.getKey()))
								{
									thisnodepro += thisnodeentry.getValue();
									asize++;
								}
									
							}
							thisnodepro = thisnodepro/asize;
							double mypro = thisnodepro/totalpro;
							if(mypro>1)
							{
								mypro = 1.0;
							}
							System.out.print(mypro+"\n");
							pw.write(mypro+"\r\n");
						}
						setnumber++;
						
						System.out.print("\n");
						fw.write("\r\n");
					}
				}
			}
		}
		fw.close();
		pw.close();
		System.out.println("Subset result has been written into file!");
	}
	
	public static void getResult(String filePath, String outPath,String proPath) {
		try {
			build_graph(filePath);
			cut_bridge();
			subgraph(outPath,proPath);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		try {
			build_graph(simpairPath);
			print_graph();
			cut_bridge();
			subgraph(resultPath,proPath);
		} catch (Exception e) {
			System.out.println("Open File Failed！" + e.toString());
		}

	}
}
