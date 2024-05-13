package database_project;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Vector;

import BTree_pkge.BTree;

public class Table implements Serializable {
	String name;
	Vector<String> pages;
	Vector<Comparable> pageMax;
	Vector<Comparable> pageMin;
	public static int totalTuples = 0;

	public Table(String tblName) {
		name = tblName;
		pages = new Vector<String>();
		pageMax = new Vector<Comparable>();
		pageMin = new Vector<Comparable>();
	}

	public static Page pdeserialize(String filename) throws DBAppException {
		Page Temp = null;
		try {
			FileInputStream file = new FileInputStream(filename);
			ObjectInputStream in = new ObjectInputStream(file);

			// Method for deserialization of object
			Temp = (Page) in.readObject();

			in.close();
			file.close();
		} catch (IOException ex) {
			throw new DBAppException("IOException is caught");
		} catch (ClassNotFoundException ex) {
			throw new DBAppException("ClassNotFoundException is caught");
		}
		return Temp;
	}

	public static Table Tbldeserialize(String filename) throws DBAppException {
		Table tableTemp = null;
		try {
			FileInputStream file = new FileInputStream(filename);
			ObjectInputStream in = new ObjectInputStream(file);

			// Method for deserialization of object
			tableTemp = (Table) in.readObject();

			in.close();
			file.close();
		} catch (IOException ex) {
			throw new DBAppException("table not found");
		} catch (ClassNotFoundException ex) {
			throw new DBAppException("ClassNotFoundException is caught");
		}
		return tableTemp;
	}

	public void tblserialize(String tblname) throws DBAppException {
		try {
			// Saving of object in a file
			FileOutputStream file = new FileOutputStream(tblname + ".ser");
			ObjectOutputStream out = new ObjectOutputStream(file);

			// Method for serialization of object
			out.writeObject(this);

			out.close();
			file.close();
		} catch (IOException ex) {
			throw new DBAppException("serialization error");
		}
	}

	public Vector<int[]> search(String value, int index) {// value could be any
		// System.out.println("in seaarch method"); // type
		Vector<int[]> res = new Vector<int[]>();
		for (int i = 0; i < pages.size(); i++) {
			Page p = Page.pdeserialize((pages).elementAt(i));
			for (int u = 0; u < p.tuples.size(); u++) {
				// System.out.println(p.tuples.size());
				Tuple d = Tuple.Tpldeserialize((String) p.tuples.elementAt(u));
				if (d.values.get(index).equals(value)) {
					int[] results = new int[2];
					results[0] = i;
					results[1] = u;
					res.add(results);
				}
			}
			// System.out.println(pages.size());
		}
		return res;
	}

	public Vector<int[]> binary_search(String value, int index) {
		Vector<int[]> results = new Vector<int[]>();
		Comparable key = DBApp.fromString(value);
		int i;
		for (i = 0; i < pages.size(); i++) {
			if (key.compareTo(DBApp.fromString(""+pageMin.get(i)))>= 0 && key.compareTo(DBApp.fromString(""+pageMax.get(i))) <= 0) {
				break;
			}
		}
		if (i == pageMin.size()) {
			return null;
		}
		Page tempPage = Page.pdeserialize(pages.get(i));
		int startp = 0;
		int endp = tempPage.tuples.size() - 1;
		int midp = (startp + endp) / 2;
		while (startp <= endp) {
			midp = (startp + endp) / 2;
			Tuple tempTuple = Tuple.Tpldeserialize((String) tempPage.tuples.get(midp));
			Comparable dvalue;
			Comparable inTable;
			try {
				dvalue = Double.parseDouble(value);
				inTable = Double.parseDouble(tempTuple.values.get(index));
			} catch (Exception e) {
				dvalue = value;
				inTable = tempTuple.values.get(index);
			}
			if (dvalue.compareTo(inTable) < 0)
				endp = midp - 1;
			if (dvalue.compareTo(inTable) > 0)
				startp = midp + 1;
			if (dvalue.compareTo(inTable) == 0) {
				int[] loc = { i, midp };
				results.add(loc);
				return results;
				// return getduplicates(i, midp, value, index);
			}

		}
		return null;
	}

	public int[] binary_search_insert(String value, int index) {
		int[] z = new int[2];
		Comparable key = DBApp.fromString(value);
		int i;
		for (i = 0; i < pages.size(); i++) {
			if (key.compareTo(pageMin.get(i)) < 0) {
				int[] loc = { i, 0 };
				return loc;
			}
			if (key.compareTo(pageMin.get(i)) >= 0 && key.compareTo(pageMax.get(i)) <= 0) {
				break;
			}
		}
		if (i == pages.size()) {
			if (i == 0) {
				int[] loc = { 0, 0 };
				return loc;
			}
			Page temp = Page.pdeserialize(pages.get(i - 1));
			if (temp.tuples.size() == Page.N) {
				int[] loc = { i, 0 };
				return loc;
			} else {
				int[] loc = { i - 1, temp.tuples.size() };
				return loc;
			}
		}
		Page tempPage = Page.pdeserialize(pages.get(i));
		int startp = 0;
		int endp = tempPage.tuples.size() - 1;
		int midp = (startp + endp) / 2;
		while (startp <= endp) {
			midp = (startp + endp) / 2;
			Tuple tempTuple = Tuple.Tpldeserialize((String) tempPage.tuples.get(midp));
			Comparable dvalue;
			Comparable inTable;
			try {
				dvalue = Double.parseDouble(value);
				inTable = Double.parseDouble(tempTuple.values.get(index));
			} catch (Exception e) {
				dvalue = value;
				inTable = tempTuple.values.get(index);
			}
			if (dvalue.compareTo(inTable) <= 0) {
				endp = midp - 1;
			}
			if (dvalue.compareTo(inTable) > 0) {
				startp = midp + 1;
			}
		}
		boolean flag = false;
		while (midp < tempPage.tuples.size()) {
			Tuple tempTuple = Tuple.Tpldeserialize((String) tempPage.tuples.get(midp));
			Comparable dvalue;
			Comparable inTable;
			try {
				dvalue = Double.parseDouble(value);
				inTable = Double.parseDouble(tempTuple.values.get(index));
			} catch (Exception e) {
				dvalue = value;
				inTable = tempTuple.values.get(index);
			}
			if (dvalue.compareTo(inTable) < 0) {
				flag = true;
				break;
			}
			midp++;
		}
		if (!flag && tempPage.tuples.size() == Page.N) {
			z[0] = i + 1;
			z[1] = 0;
		} else {
			z[0] = i;
			z[1] = midp;
		}
		return z;
	}

	public Vector<int[]> binarySearchGreater(String value, int index) {
		Vector<int[]> results = new Vector<int[]>();
		int[] tmp = this.binary_search_insert(value, index);
		for (int i = tmp[0]; i < this.pages.size(); i++) {
			Page currPage = Page.pdeserialize(pages.get(i));
			for (int j = 0; j < currPage.tuples.size(); j++) {
				int tupleIndex = j;
				if (i == tmp[0]) {
					tupleIndex = tmp[1] + j;
					if (j == (currPage.tuples.size() - tmp[1]))
						break;
				}
				int[] curr = { i, tupleIndex };
				results.add(curr);
			}
		}
		return results;
	}

	public Vector<int[]> binarySearchSmaller(String value, int index) {
		Vector<int[]> results = new Vector<int[]>();
		int[] tmp = this.binary_search_insert(value, index);
		// Vector<int[]> b_search = this.binary_search(value, index);

		for (int i = 0; i <= tmp[0]; i++) {
			Page currPage = Page.pdeserialize(pages.get(i));
			for (int j = 0; j < currPage.tuples.size(); j++) {
				if (i == tmp[0] && j == tmp[1]) {
					break;
				}
				int[] curr = { i, j };
				results.add(curr);
			}
		}
		if (this.binary_search(value, index) != null) {
			results.remove(results.size() - 1);
		}
		return results;
	}

	public Vector<int[]> search_greater(String value, int index) {// value could be any
		// System.out.println("in search method"); // type
		Vector<int[]> res = new Vector<int[]>();
		Comparable parsed_value = DBApp.fromString(value);
		for (int i = 0; i < pages.size(); i++) {
			Page p = Page.pdeserialize((pages).elementAt(i));
			for (int u = 0; u < p.tuples.size(); u++) {
				// System.out.println(p.tuples.size());
				Tuple d = Tuple.Tpldeserialize((String) p.tuples.elementAt(u));
				Comparable cur_value = DBApp.fromString(d.values.get(index));
				if (cur_value.compareTo(parsed_value) > 0) {
					int[] results = new int[2];
					results[0] = i;
					results[1] = u;
					res.add(results);
				}
			}
			// System.out.println(pages.size());
		}
		return res;
	}

	public Vector<int[]> search_smaller(String value, int index) {// value could be any
		// System.out.println("in search method"); // type
		Vector<int[]> res = new Vector<int[]>();
		Comparable parsed_value = DBApp.fromString(value);
		for (int i = 0; i < pages.size(); i++) {
			Page p = Page.pdeserialize((pages).elementAt(i));
			for (int u = 0; u < p.tuples.size(); u++) {
				// System.out.println(p.tuples.size());
				Tuple d = Tuple.Tpldeserialize((String) p.tuples.elementAt(u));
				Comparable cur_value = DBApp.fromString(d.values.get(index));
				if (cur_value.compareTo(parsed_value) < 0) {
					int[] results = new int[2];
					results[0] = i;
					results[1] = u;
					res.add(results);
				}
			}
			// System.out.println(pages.size());
		}
		return res;
	}

	public Vector<int[]> select_search(String value, int index, String operator) throws IOException, DBAppException {
		Vector<int[]> results = new Vector<int[]>();
		try {
		Vector<String> isPk = DBApp.getIfPK(this.name);
		Vector<String> IndexNames = DBApp.getIndexName(this.name);
		Vector<String> colTypes = DBApp.getColTypes(this.name);
		Comparable BTval = null;
		BTree btree = null;
		boolean BTreeFlag = false;
		boolean primaryFlag = false;
		boolean LinearFlag = false;

		// checks what type of search to do
		if (!IndexNames.get(index).equals("null")) {
			// use btree
			BTreeFlag = true;
			btree = BTree.deserialize(IndexNames.get(index) + ".ser");
			BTval = DBApp.convertToType(value, colTypes.get(index));
		} else if ((IndexNames.get(index).toUpperCase()).equals("TRUE")) {
			primaryFlag = true;
		} else {
			LinearFlag = true;
		}

		if (operator.equals("=")) {
			if (BTreeFlag) {
				results = (Vector<int[]>) btree.search(BTval);
			}
			if (primaryFlag) {
				results = this.binary_search(value, index);
			}
			if (LinearFlag) {
				results = this.search(value, index);
			}
		}
		else if (operator.equals("<")) {
			if (BTreeFlag) {
				results = btree.searchSmaller(BTval);
			}
			if (primaryFlag) {
				results = this.binarySearchSmaller(value, index);
			}
			if (LinearFlag) {
				results = this.search_smaller(value, index);
			}

		}
		else if (operator.equals(">")) {
			if (BTreeFlag) {
				results = btree.searchGreater(BTval);
			}
			if (primaryFlag) {
				results = this.binarySearchGreater(value, index);
			}
			if (LinearFlag) {
				results = this.search_greater(value, index);
			}
		}
		else if (operator.equals("<=")) {
			if (BTreeFlag) {
				results = DBApp.getOR(btree.searchSmaller(BTval), (Vector<int[]>) btree.search(BTval));
			}
			if (primaryFlag) {
				results = DBApp.getOR(this.binarySearchSmaller(value, index), this.binary_search(value, index));

			}
			if (LinearFlag) {
				results = DBApp.getOR(this.search_smaller(value, index), this.search(value, index));
			}
		}
		else if (operator.equals(">=")) {
			if (BTreeFlag) {
				results = DBApp.getOR((Vector<int[]>) btree.search(BTval), btree.searchGreater(BTval));
			}
			if (primaryFlag) {
				results = DBApp.getOR(this.binary_search(value, index), this.binarySearchGreater(value, index));
			}
			if (LinearFlag) {
				results = DBApp.getOR(this.search(value, index), this.search_greater(value, index));
			}
		}
		else if (operator.equals("!=")) {
			// put all table in vector to be filtered
			Vector<int[]> allTuples = new Vector<int[]>();
			for (int i = 0; i < this.pages.size(); i++) {
				Page currP = Page.pdeserialize(this.pages.get(i));
				for (int j = 0; j < currP.tuples.size(); j++) {
					int[] tmp = { i, j };
					allTuples.add(tmp);
				}
				// currP.serialize(operator);
			}

			Vector<int[]> equalRes = new Vector<int[]>();
			if (BTreeFlag) {
				equalRes = (Vector<int[]>) btree.search(BTval);
			}
			if (primaryFlag) {
				equalRes = this.binary_search(value, index);

			}
			if (LinearFlag) {
				equalRes = this.search(value, index);

			}
			int k = 0;
			while (k < allTuples.size()) {
				for (int j = 0; j < equalRes.size(); j++) {
					if (allTuples.get(k)[0] == equalRes.get(j)[0] && allTuples.get(k)[1] == equalRes.get(j)[1]) {
						allTuples.remove(k);
						k--;
						break;
					}
				}
				k++;
			}
			return allTuples;
		}
		else {
			throw new DBAppException("invalid select statement");
		}
		}
		catch(IOException e) {
			throw new DBAppException("files not found");	
		}

		return results;
	}


}