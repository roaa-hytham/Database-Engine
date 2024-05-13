package database_project;

/** * @author Wael Abouelsaadat */

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import BTree_pkge.BTree;

public class DBApp {

	// 2 vectors or 1 hashtable to hold table name and its serialized filename
	public static Hashtable<String, String> tblname; // would need to be

	// serialized and
	// deserialized later

	public DBApp() {
		// tblname = new Hashtable<String, String>();
	}

	// this does whatever
	// initialization you would like
	// or leave it empty if there is no code you want to
	// execute at application startup
	public void init() {
		// deserialize hashtable tblname

	}

	public static int getColIndex(String strTableName, String columnName)
			throws IOException, DBAppException {
		int index = -1;
		try {
			FileReader fileRead = new FileReader("Meta-Data.csv");
			BufferedReader buffRead = new BufferedReader(fileRead);
			String lineRead = null;
			while ((lineRead = buffRead.readLine()) != null) {
				String[] tempArr = lineRead.split(",");
				if (tempArr[0].equals(strTableName)) {
					index++;
					if (tempArr[1].equals(columnName)) {
						break;
					}
				}
			}
		} catch (Exception e) {
			throw new DBAppException("file not found");
		}
		return index;
	}

	public static Vector<String> getIndexName(String strTableName)
			throws IOException, DBAppException {
		@SuppressWarnings("unchecked")
		Vector<String> indexName = new Vector<String>();

		try {
			FileReader fileRead = new FileReader("Meta-Data.csv");
			BufferedReader buffRead = new BufferedReader(fileRead);
			String lineRead = null;
			int i = 0;
			while ((lineRead = buffRead.readLine()) != null) {
				String[] tempArr = lineRead.split(",");
				if (tempArr[0].equals(strTableName)) {
					indexName.add(tempArr[4]);
					i++;
				}
			}
		} catch (Exception e) {
			throw new DBAppException("file not found");
		}
		return indexName;

	}

	public static Vector<String> getColTypes(String strTableName)
			throws IOException, DBAppException {
		Vector<String> results = new Vector<String>();
		try {
			FileReader fileRead = new FileReader("Meta-Data.csv");
			BufferedReader buffRead = new BufferedReader(fileRead);
			String lineRead = null;
			while ((lineRead = buffRead.readLine()) != null) {
				String[] tempArr = lineRead.split(",");
				if (tempArr[0].equals(strTableName)) {
					results.add(tempArr[2]);
				}
			}
		} catch (Exception e) {
			throw new DBAppException("file not found");
		}
		return results;
	}

	public static Vector<String> getCols(String strTableName)
			throws DBAppException {
		Vector<String> results = new Vector<String>();
		try {
			FileReader fileRead = new FileReader("Meta-Data.csv");
			BufferedReader buffRead = new BufferedReader(fileRead);
			String lineRead = null;
			while ((lineRead = buffRead.readLine()) != null) {
				String[] tempArr = lineRead.split(",");
				if (tempArr[0].equals(strTableName)) {
					results.add(tempArr[1]);
				}
			}
		} catch (Exception e) {
			throw new DBAppException("file not found");
		}
		return results;
	}

	public static Vector<String> getIfPK(String strTableName)
			throws DBAppException {
		Vector<String> results = new Vector<String>();
		try {
			FileReader fileRead = new FileReader("Meta-Data.csv");
			BufferedReader buffRead = new BufferedReader(fileRead);
			String lineRead = null;
			while ((lineRead = buffRead.readLine()) != null) {
				String[] tempArr = lineRead.split(",");
				if (tempArr[0].equals(strTableName)) {
					results.add(tempArr[3]);
				}
			}
		} catch (Exception e) {
			throw new DBAppException("file not found");
		}
		return results;
	}

	public static void checkType(Object value, String type)
			throws DBAppException {
		if (value instanceof Double
				&& (type.equals("java.lang.double") || type
						.equals("java.lang.Double"))) {
			return;
		} else if (value instanceof Integer && type.equals("java.lang.Integer")) {
			return;
		} else if (value instanceof String && type.equals("java.lang.String")) {
			return;
		} else {
			throw new DBAppException("invalid input type");
		}

	}

	public static Comparable convertToType(String value, String type)
			throws IOException {
		Comparable result = value;

		if (type.equals("java.lang.double")) {
			result = Double.parseDouble(value);
		}
		if (type.equals("java.lang.Integer")) {
			result = Integer.parseInt(value);
		}
		return result;
	}

	public static Comparable fromString(String s) {
		Comparable key;
		try {
			key = Integer.parseInt(s);
		} catch (Exception e) {
			try {
				key = Double.parseDouble(s);
			} catch (Exception r) {
				key = s;
			}
		}
		return key;
	}

	public static double convertTo(String z) {
		String x = z.toLowerCase();
		int y = 1;
		double result = 0;
		for (int i = 0; i < x.length(); i++) {
			result += x.charAt(i) * Math.pow(10, y);
		}
		return result;
	}

	public static int readNumberFromConfig() {
		Properties prop = new Properties();
		try {
			FileInputStream fileInputStream = new FileInputStream(
					"DBApp.config");
			prop.load(fileInputStream);
			String property = prop.getProperty("MaximumRowsCountinPage");
			if (property != null) {
				return Integer.parseInt(property);
			} else {
				System.err
						.println("Property 'MaximumRowsCountinPage' not found in the configuration file.");
				return -1; // Or handle the error as appropriate
			}
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}
	}

	// following method creates one table only
	// strClusteringKeyColumn is the name of the column that will be the primary
	// key and the clustering column as well. The data type of that column will
	// be passed in htblColNameType
	// htblColNameValue will have the column name as key and the data
	// type as value
	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException {

		try {
			FileReader fileRead = new FileReader("Meta-Data.csv");
			BufferedReader buffRead = new BufferedReader(fileRead);
			String lineRead = null;
			while ((lineRead = buffRead.readLine()) != null) {
				String[] tempArr = lineRead.split(",");
				if (tempArr[0].equals(strTableName)) {
					throw new DBAppException("Table already exists");
				}
			}
		} catch (IOException e) {
			throw new DBAppException("Problem in Meta Data Reader");
		}

		Table tbl = new Table(strTableName);

		// Adding to csv file!

		String FilePath = "Meta-Data.csv";

		try {
			// Create a FileWriter to write to the CSV file
			FileWriter fileWriter1 = new FileWriter(FilePath, true);

			// Create a BufferedWriter to improve writing performance
			BufferedWriter bufferedWriter = new BufferedWriter(fileWriter1);

			// Storing all entries of Hashtable in a Set
			// using entrySet() method
			Set<java.util.Map.Entry<String, String>> entrySet = htblColNameType
					.entrySet();

			// Creating an Iterator object to
			// iterate over the given Hashtable
			Iterator<java.util.Map.Entry<String, String>> itr = entrySet
					.iterator();

			// Iterating through the Hashtable object
			// using iterator

			// Checking for next element
			// using hasNext() method
			while (itr.hasNext()) {
				boolean isPK = false;
				// Getting a particular entry of HashTable
				java.util.Map.Entry<String, String> entry = itr.next();
				if (entry.getKey().equals(strClusteringKeyColumn)) {
					isPK = true;
				}
				// Table Name, Column Name, Column Type, ClusteringKey,
				// IndexName,IndexType
				bufferedWriter.write(strTableName + "," + entry.getKey() + ","
						+ entry.getValue() + "," + isPK + "," + "null" + ","
						+ "null");
				bufferedWriter.newLine(); // Move to the next line
			}

			// Close the BufferedWriter
			bufferedWriter.close();
			System.out.println("Table Created Successfully");

		} catch (IOException e) {
			throw new DBAppException("process unseccessful");
		}

		// serialize table
		tbl.tblserialize(strTableName);
	}

	public static void updateBTree(String strTableName, String strIndexName,
			String colType, int index) throws DBAppException {
		BTree t;
		if (colType.equals("java.lang.double"))
			t = new BTree<Double, Vector<int[]>>();
		else if (colType.equals("java.lang.Integer"))
			t = new BTree<Integer, Vector<int[]>>();
		else
			t = new BTree<String, Vector<int[]>>();
		Table table = null;
		try {
			table = Table.Tbldeserialize(strTableName + ".ser");
		} catch (DBAppException e) {
			throw new DBAppException("table not found");
		}
		for (int y = 0; y < table.pages.size(); y++) {
			Page p = Page.pdeserialize((table.pages).elementAt(y));
			for (int u = 0; u < p.tuples.size(); u++) {
				Tuple d = Tuple.Tpldeserialize((String) p.tuples.elementAt(u));
				Comparable key;
				if (colType.equals("java.lang.double"))
					key = Double.parseDouble(d.values.get(index));
				else if (colType.equals("java.lang.Integer"))
					key = Integer.parseInt(d.values.get(index));
				else
					key = d.values.get(index);
				double stringTo;
				Vector<int[]> value;
				;
				value = (Vector<int[]>) t.search(key);

				int[] currloc = { y, u };
				if (value == null)
					value = new Vector<int[]>();
				value.add(currloc);
				t.insert(key, value);
			}
		}
		t.print();
		t.serialize(strIndexName);
	}

	// following method creates a B+tree index
	public void createIndex(String strTableName, String strColName,
			String strIndexName) throws DBAppException {
		try {
			FileReader fileRead = new FileReader("Meta-Data.csv");
			BufferedReader buffRead = new BufferedReader(fileRead);
			String lineRead = null;
			boolean foundT = false;
			boolean foundC = false;
			while ((lineRead = buffRead.readLine()) != null) {
				String[] tempArr = lineRead.split(",");
				if (tempArr[0].equals(strTableName)) {
					foundT = true;

					if (tempArr[1].equals(strColName)) {
						foundC = true;
					}
				}
			}
			if (!foundT)
				throw new DBAppException("Table doesn't exist");
			if (foundC == false)
				throw new DBAppException("column name doesn't exist");
		} catch (IOException e) {
			throw new DBAppException("Problem in Meta Data Reader");
		}
		try {
			String filePath = "Meta-Data.csv";
			BufferedReader reader = new BufferedReader(new FileReader(filePath));
			String line;
			StringBuilder fileContent = new StringBuilder();
			String colType = "java.lang.String";
			int i = -1;
			int colnum = 0;

			while ((line = reader.readLine()) != null) {
				String[] parts = line.split(",");
				if (parts[0].equals(strTableName)) {
					i++;
				}
				if (parts[0].equals(strTableName)
						&& parts[1].equals(strColName)) {
					parts[4] = strIndexName;
					parts[5] = "B+Tree";
					colType = parts[2];
					colnum = i;
				}
				fileContent.append(String.join(",", parts)).append("\n");
			}

			reader.close();

			// Write back to the file
			BufferedWriter writer = new BufferedWriter(new FileWriter(filePath));
			writer.write(fileContent.toString());
			writer.close();

			BTree t;
			if (colType.equals("java.lang.double"))
				t = new BTree<Double, Vector<int[]>>();
			else if (colType.equals("java.lang.Integer"))
				t = new BTree<Integer, Vector<int[]>>();
			else
				t = new BTree<String, Vector<int[]>>();

			Table table = Table.Tbldeserialize(strTableName + ".ser");
			for (int y = 0; y < table.pages.size(); y++) {
				Page p = Page.pdeserialize((table.pages).elementAt(y));
				for (int u = 0; u < p.tuples.size(); u++) {
					Tuple d = Tuple.Tpldeserialize((String) p.tuples
							.elementAt(u));
					Comparable key;
					if (colType.equals("java.lang.double"))
						key = Double.parseDouble(d.values.get(colnum));
					else if (colType.equals("java.lang.Integer"))
						key = Integer.parseInt(d.values.get(colnum));
					else
						key = d.values.get(colnum);
					double stringTo;
					Vector<int[]> value;

					value = (Vector<int[]>) t.search(key);

					int[] currloc = { y, u };
					if (value == null)
						value = new Vector<int[]>();
					value.add(currloc);
					t.insert(key, value);
				}
			}
			t.serialize(strIndexName);

		} catch (IOException e) {
			throw new DBAppException("file not found");
		}

	}

	public void shiftBetPages(String strTableName, Tuple tuple, int[] location,
			int indexPK) throws DBAppException {
		Table tb = Table.Tbldeserialize(strTableName + ".ser");
		int tableSize = tb.pages.size();
		int pageNum = location[0];
		int tupleNum = location[1];
		int currPage = pageNum;
		Page p;
		boolean flag = false;
		String shiftedTuple = null;
		boolean first_flag = false;
		while (currPage != tableSize) {
			p = Page.pdeserialize(tb.pages.get(currPage));
			if (flag && !first_flag) {
				p.tuples.add(0, shiftedTuple);
				Tuple first = Tuple.Tpldeserialize((String) p.tuples
						.firstElement());
				Comparable min = fromString(first.values.get(indexPK));
				Tuple last = Tuple.Tpldeserialize((String) p.tuples
						.lastElement());
				Comparable max = fromString(last.values.get(indexPK));
				tb.pageMin.remove(currPage);
				tb.pageMax.remove(currPage);
				tb.pageMin.add(currPage, min);
				tb.pageMax.add(currPage, max);
				first_flag = true;
			}
			if ((p.N == p.tuples.size() && !first_flag)
					|| (p.N + 1 == p.tuples.size() && first_flag)) {
				if (!flag) {
					tuple.serialize(tuple.name);
					p.tuples.add(tupleNum, tuple.name + ".ser");
					Tuple first = Tuple.Tpldeserialize((String) p.tuples
							.firstElement());
					Comparable min = fromString(first.values.get(indexPK));
					Tuple last = Tuple.Tpldeserialize((String) p.tuples
							.lastElement());
					Comparable max = fromString(last.values.get(indexPK));
					tb.pageMin.remove(currPage);
					tb.pageMax.remove(currPage);
					tb.pageMin.add(currPage, min);
					tb.pageMax.add(currPage, max);
					first.serialize(strTableName);
					last.serialize(strTableName);

				}
				shiftedTuple = (String) p.tuples.lastElement();
				p.tuples.remove(p.N);
				Tuple first = Tuple.Tpldeserialize((String) p.tuples
						.firstElement());
				Comparable min = fromString(first.values.get(indexPK));
				Tuple last = Tuple.Tpldeserialize((String) p.tuples
						.lastElement());
				Comparable max = fromString(last.values.get(indexPK));
				tb.pageMin.remove(currPage);
				tb.pageMax.remove(currPage);
				tb.pageMin.add(currPage, min);
				tb.pageMax.add(currPage, max);
				first.serialize(strTableName);
				last.serialize(strTableName);
				flag = true;
			} else {
				flag = false;
				p.serialize(p.name);
				break;
			}
			currPage++;
			p.serialize(p.name);
			tb.tblserialize(strTableName);
			first_flag = false;
		}
		if (flag) {

			Page newPage = new Page(strTableName, tableSize);
			tb = Table.Tbldeserialize(strTableName + ".ser");
			newPage.tuples.add(shiftedTuple);
			Tuple first = Tuple.Tpldeserialize((String) newPage.tuples
					.firstElement());
			Comparable min = fromString(first.values.get(indexPK));
			Tuple last = Tuple.Tpldeserialize((String) newPage.tuples
					.lastElement());
			Comparable max = fromString(last.values.get(indexPK));
			tb.pageMin.remove(currPage);
			tb.pageMax.remove(currPage);
			tb.pageMin.add(currPage, min);
			tb.pageMax.add(currPage, max);

			newPage.serialize(newPage.name);
		}
		tb.tblserialize(strTableName);

	}

	// following method inserts one row only.
	// htblColNameValue must include a value for the primary key
	// public void changeName(Tuple t) {
	// if()
	// }
	public void insertIntoTable(String strTableName,
			Hashtable<String, Object> htblColNameValue) throws DBAppException,
			IOException {
		// use binary_search if value in table ,otherwise binary_search_insert
		// deserialize file based on table name "strTableName.ser"
		Table table = null;
		try {
			table = Table.Tbldeserialize(strTableName + ".ser");
		} catch (Exception e) {
			throw new DBAppException("table not found");
		}

		int i = 0;
		int indexPK = 0;
		// read from csv file if it is indexed
		FileReader fileRead = new FileReader("Meta-Data.csv");
		BufferedReader buffRead = new BufferedReader(fileRead);
		String lineRead;
		String[] tempArr = null;
		Vector<String> currtuple = new Vector<String>();
		Vector<Boolean> hasBTree = new Vector<Boolean>();
		Vector<Object> values = new Vector<Object>();
		Vector<String> dataTypes = new Vector<String>();
		Vector<String> ifPK = new Vector<String>();
		Vector<String> BTreeNames = new Vector<String>();
		Vector<String> colNames = getCols(strTableName);
		boolean colExists = false;

		while ((lineRead = buffRead.readLine()) != null) {
			tempArr = lineRead.split(",");

			// for (int j = 0; j < colNames.size(); j++) {
			// if (colNames.get(i).equals())
			// colExists = true;
			// }
			// if (!colExists) {
			// throw new DBAppException("no column with name " +
			// htblColNameValue.get(tempArr[1]) + " exists");
			// }
			if (tempArr[0].equals(strTableName)) {
				String currvalue = "" + htblColNameValue.get(tempArr[1]);
				currtuple.add(currvalue);
				values.add(htblColNameValue.get(tempArr[1]));
				if (tempArr[5].equals("null"))
					hasBTree.add(false);
				else {
					hasBTree.add(true);
				}
				if (!tempArr[3].equals("FALSE"))
					indexPK = i;
				i++;
				BTreeNames.add(tempArr[4]);
			}

		}
		// test validity of inputs
		dataTypes = getColTypes(strTableName);

		if (dataTypes.size() != values.size()) {
			throw new DBAppException("invalid size of tuple");
		}

		for (int j = 0; j < values.size(); j++) {
			if (values.get(j) == null) {
				throw new DBAppException("cannot enter null values");
			}
			try {
				checkType(values.get(j), dataTypes.get(j));
			} catch (DBAppException e) {
				throw new DBAppException("wrong data type");
			}
		}

		Page pp;
		if (table.binary_search(currtuple.get(indexPK), indexPK) != null) {
			throw new DBAppException(
					"Primary Key Value already exists. Can't insert");
		}

		int[] insertAt = table.binary_search_insert(currtuple.get(indexPK),
				indexPK);

		if (table.pages.size() == 0 || insertAt[0] >= table.pages.size()) { // creating
																			// new
																			// page
			pp = new Page(strTableName, table.pages.size());
			table = Table.Tbldeserialize(strTableName + ".ser");
			pp.serialize(pp.name);
			table.tblserialize(strTableName);
		} else {

			pp = Page.pdeserialize(table.pages.get(insertAt[0]));

		}

		Tuple newTup = new Tuple(pp);
		newTup.values = currtuple;

		if (pp.tuples.size() >= pp.N) {
			//
			shiftBetPages(strTableName, newTup, insertAt, indexPK);

		} else {
			newTup.serialize(newTup.name);
			pp.tuples.add(insertAt[1], newTup.name + ".ser");
			Tuple first = Tuple.Tpldeserialize((String) pp.tuples
					.firstElement());
			Comparable min = fromString(first.values.get(indexPK));
			Tuple last = Tuple.Tpldeserialize((String) pp.tuples.lastElement());
			Comparable max = fromString(last.values.get(indexPK));
			table.pageMin.remove(insertAt[0]);
			table.pageMax.remove(insertAt[0]);
			table.pageMin.add(insertAt[0], min);
			table.pageMax.add(insertAt[0], max);
			pp.serialize(pp.name);
			table.tblserialize(strTableName);
		}
		Vector<String> ColTypes = getColTypes(strTableName);
		for (int k = 0; k < hasBTree.size(); k++) {
			String TypeOfCol = ColTypes.get(k);
			if (hasBTree.get(k)) {
				updateBTree(strTableName, BTreeNames.get(k), TypeOfCol, k);
			}
		}

		table.totalTuples++;

	}

	// following method updates one row only
	// htblColNameValue holds the key and new valuein
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the row to update.
	public void updateTable(String strTableName, String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue) throws DBAppException,
			IOException {
		Table table = null;
		try {

			int clusteringIndex = this.getColIndex(strTableName,
					strClusteringKeyValue);
			Vector<String> htblColName = new Vector<String>();
			Vector<Object> htblValues = new Vector<Object>();
			Vector<String> isPK = getIfPK(strTableName);
			// deserialize table and search for tuples to update
			try {
				table = Table.Tbldeserialize(strTableName + ".ser");
			} catch (DBAppException e) {
				throw new DBAppException("table not found");
			}
			Vector<int[]> findPK = table.binary_search(strClusteringKeyValue,
					clusteringIndex);
			// handles pk duplicates
			if (findPK == null || findPK.isEmpty()) {
				throw new DBAppException("tuple doesn't exist");
			}
			Vector<int[]> tuple_index = table.binary_search(
					strClusteringKeyValue, clusteringIndex);
			// array for knowing if columns are indexed
			Vector<String> colIndexeName = getIndexName(strTableName);
			Vector<String> colType = getColTypes(strTableName);
			// get values from hashtable
			for (Map.Entry<String, Object> entry : htblColNameValue.entrySet()) {
				// Extract the key (column name) from the entry
				String colName = entry.getKey();
				// check that column exists in table
				boolean colExists = false;
				Vector<String> colNames = getCols(strTableName);
				for (int i = 0; i < colNames.size(); i++) {
					if (colNames.get(i).equals(colName))
						colExists = true;
				}
				if (!colExists) {
					throw new DBAppException("no column with name " + colName
							+ " exists");
				}
				htblColName.add(colName);

				int colIndex = getColIndex(strTableName, colName);

				if (isPK.get(colIndex).toUpperCase().equals("TRUE")) {
					throw new DBAppException("cant update primary key");
				}

				Object colvalue = entry.getValue();
				// wrong data type
				String s = colType.get(colIndex);
				try {
					checkType(colvalue, s);
				} catch (DBAppException e) {
					throw new DBAppException("invalid input");
				}
				htblValues.add(colvalue);
			}

			// handles duplicates
			if (tuple_index == null)
				return;
			for (int i = 0; i < tuple_index.size(); i++) {
				int[] tmp = tuple_index.get(i);
				Page page = Page.pdeserialize(table.pages.get(tmp[0]) + "");
				Tuple tuple = Tuple
						.Tpldeserialize(page.tuples.get(tmp[1]) + "");
				// loops over values to be updated inside same tuple
				for (int j = 0; j < htblColName.size(); j++) {
					int keyIndex = this.getColIndex(strTableName,
							htblColName.get(j));

					String oldValue = tuple.values.get(keyIndex);
					String newValue = htblValues.get(j) + "";
					// update index if there is one
					if (!colIndexeName.get(keyIndex).equals("null")) {
						BTree btree = BTree.deserialize(colIndexeName
								.get(keyIndex) + ".ser");
						Comparable Old = oldValue;
						Comparable New = newValue;
						if (colType.get(keyIndex).equals("java.lang.double")) {
							Old = Double.parseDouble(oldValue);
							New = Double.parseDouble(newValue);
						}
						if (colType.get(keyIndex).equals("java.lang.Integer")) {
							Old = Integer.parseInt(oldValue);
							New = Integer.parseInt(newValue);
						}
						// update vector for old value
						Vector<int[]> oldValLocations = (Vector<int[]>) btree
								.search(Old);
						oldValLocations.remove(tmp);
						btree.delete(Old);
						btree.insert(Old, oldValLocations);
						// update vector for new value
						Vector<int[]> newValLocations = (Vector<int[]>) btree
								.search(New);
						if (newValLocations == null)
							newValLocations = new Vector<int[]>();
						newValLocations.add(tmp);
						btree.delete(New);
						btree.insert(New, newValLocations);
					}
					// update tuple
					tuple.values.remove(keyIndex);
					tuple.values.add(keyIndex, htblValues.get(j) + "");

				}
				tuple.serialize(tuple.name);
				page.serialize(page.name);
			}
			table.tblserialize(strTableName);
		} catch (IOException e) {
			throw new DBAppException("table not found");
		}

		// throw new DBAppException("not implemented yet");
	}

	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search
	// to identify which rows/tuples to delete.
	// htblColNameValue enteries are ANDED together
	public void deleteFromTable(String strTableName,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {
		Table table = Table.Tbldeserialize(strTableName + ".ser");
		Vector<String> htblColName = new Vector<String>();
		Vector<Object> htblValues = new Vector<Object>();

		// get values from hashtable
		for (Map.Entry<String, Object> entry : htblColNameValue.entrySet()) {
			// Extract the key (column name) from the entry
			String colName = entry.getKey();
			htblColName.add(colName);
			Object colvalue = entry.getValue();
			htblValues.add(colvalue);
		}

		try {
			Vector<Integer> htblColIndex = new Vector<Integer>();
			FileReader fileRead = new FileReader("Meta-Data.csv");
			BufferedReader buffRead = new BufferedReader(fileRead);
			String lineRead = null;
			String BColName = null;
			int BColNameIndex = -1;
			String strIndexName = null;
			String primarykey = null;
			boolean foundT = false;
			Vector<String> allCol = new Vector<String>();
			Vector<String> ColType = new Vector<String>();
			Vector<Object> Colvalue = new Vector<Object>();

			int index = -1;
			int u = 0;
			while ((lineRead = buffRead.readLine()) != null) {
				String[] tempArr = lineRead.split(",");
				if (tempArr[0].equals(strTableName)) {
					BColNameIndex++;
					index++;
					foundT = true;
					allCol.add(tempArr[1]);
				}
				if (tempArr[0].equals(strTableName)
						&& htblColName.contains(tempArr[1])) {
					htblColIndex.add(index);
					u++;
					if (tempArr[5].equals("B+Tree")) {
						BColName = tempArr[1];
						strIndexName = tempArr[4];
					}
					if (tempArr[3].toUpperCase().equals("TRUE")) {
						primarykey = tempArr[1];
					}
					ColType.add(tempArr[2]);
					Colvalue.add(htblColNameValue.get(tempArr[1]));
				}

			}
			Vector<String> dataTypes = getColTypes(strTableName);
			if (!foundT)
				throw new DBAppException("Table doesn't exist");
			for (int i = 0; i < htblColName.size(); i++) {
				if (!allCol.contains(htblColName.get(i)))
					throw new DBAppException("no column with name "
							+ htblColName.get(i) + " exists");
			}
			for (int j = 0; j < Colvalue.size(); j++) {
				try {
					checkType(Colvalue.get(j), ColType.get(j));
				} catch (DBAppException e) {
					throw new DBAppException("wrong data type");
				}
				if (htblValues.get(j) == null) {
					throw new DBAppException("cannot enter null values");
				}
			}
			for (int i = 0; i < table.pages.size(); i++) {
				Page pgTemp = Page.pdeserialize(table.pages.get(i));
				for (int j = 0; j < pgTemp.tuples.size(); j++) {
					Tuple tplTemp = Tuple.Tpldeserialize((String) pgTemp.tuples
							.get(j));
					System.out.println(tplTemp.toString() + "[" + i + " , " + j
							+ "]");
				}
			}

			// step 1: file is read^
			// step 2 V needs to store tuples that satisfy 1st condition
			Vector<int[]> V = new Vector<int[]>();

			if (BColName != null) {
				BTree bb = BTree.deserialize(strIndexName + ".ser");
				V = (Vector<int[]>) bb.search((Comparable) htblColNameValue
						.get(BColName));
				bb.delete((Comparable) htblColNameValue.get(BColName));

			} else if (primarykey != null) {
				String valueOfPK = "" + htblColNameValue.get(primarykey);
				V = table.binary_search(valueOfPK, BColNameIndex);// store in
				// variable(vector
				// or
				// iterator)//Help!!
			}

			else {
				if (htblColIndex.size() == 0) {
					for (int i = 0; i < table.pages.size(); i++) {
						Page tempPage = Page.pdeserialize(table.pages.get(i));
						for (int j = 0; j < tempPage.tuples.size(); j++) {
							int[] loca = { i, j };
							V.add(loca);
						}
					}
				} else {
					V = table.search(htblValues.get(0) + "",
							(int) htblColIndex.get(0));
				}
			}

			// V^^Table is search linearly with values of contidtion 1
			int i = -1;

			// searches based on first condition
			int m = 0;
			if (V == null)
				return;
			for (int j = 0; j < V.size(); j++) {
				int[] temp = V.get(j);
				Page pg = Page.pdeserialize(strTableName + temp[0] + ".ser");
				Tuple tp = Tuple.Tpldeserialize(pg.tuples.elementAt(temp[1])
						+ "");
				int conditions = 1; // to loop over conditions

				while (conditions < htblValues.size()) {

					System.out.println("value in table:"
							+ tp.values.get(htblColIndex.get(conditions)));
					System.out.println("value checking on:"
							+ htblValues.get(conditions));
					if (!tp.values.get(htblColIndex.get(conditions)).equals(
							"" + htblValues.get(conditions))) {
						System.out.println("from index"
								+ tp.values.get(htblColIndex.get(conditions)));
						System.out.println("from values"
								+ htblValues.get(conditions));
						System.out.println("Whats deleted: " + V.get(j)[0]
								+ V.get(j)[1]);
						V.remove(j);// remove if any condition not satisfied
						System.out.println("M; " + m);
						m++;
						break;
					}
					conditions++;
				}
				tp.serialize(pg.tuples.elementAt(temp[1]) + "");
				pg.serialize(table.pages.get(temp[0]));

			}
			int countDlt = 0;
			int delPages = 0;
			int prevPage = 0;
			if (V.size() != 0)
				prevPage = V.get(0)[0];
			for (int j = 0; j < V.size(); j++) {
				Table tblTamp = Table.Tbldeserialize(strTableName + ".ser");
				int[] temp = V.get(j);
				Page pg = Page
						.pdeserialize(table.pages.get(temp[0] - delPages));
				boolean flag = false;
				if ((temp[1] - countDlt) == 0) {
					table.pageMin.remove((temp[0] - delPages));
					Tuple first = Tuple.Tpldeserialize((String) pg.tuples
							.firstElement());
					table.pageMin
							.add((temp[0] - delPages), fromString(first.values
									.get(indexPK(strTableName))));
				} else if ((temp[1] - countDlt) == pg.tuples.size() - 1) {
					table.pageMax.remove((temp[0] - delPages));
					Tuple last = Tuple.Tpldeserialize((String) pg.tuples
							.lastElement());
					table.pageMax.add((temp[0] - delPages),
							fromString(last.values.get(indexPK(strTableName))));
					System.out.println("page min class in delete:"
							+ table.pageMin.get(temp[0] - delPages).getClass());
				}
				if (prevPage != temp[0]) {
					countDlt = 0;
					prevPage = temp[0];
				}
				if (temp[1] >= 0 && (temp[1] - countDlt) < pg.tuples.size()) {
					Tuple temptpl = Tuple.Tpldeserialize((String) pg.tuples
							.elementAt(temp[1] - countDlt));
				}

				pg.tuples.remove(temp[1] - countDlt);
				pg.serialize(pg.name);
				Page p = Page.pdeserialize(table.pages.get(temp[0] - delPages));
				if (pg.tuples.size() == 0) {
					table.pages.remove(temp[0] - delPages);
					table.pageMin.remove(temp[0] - delPages);
					table.pageMax.remove(temp[0] - delPages);
					delPages++;
				}
				table.tblserialize(strTableName);
				table = Table.Tbldeserialize(strTableName + ".ser");
				System.out.println(p.tuples.size());
				countDlt++;

			}
			// end of for loop to remove
			Vector<String> ColTypes = getColTypes(strTableName);
			Vector<String> indexNames = getIndexName(strTableName);
			for (int k = 0; k < indexNames.size(); k++) {
				String TypeOfCol = ColTypes.get(k);
				if (!indexNames.equals("null")) {
					updateBTree(strTableName, indexNames.get(k), TypeOfCol, k);
				}
			}

			Table tblTamp = Table.Tbldeserialize(strTableName + ".ser");
			for (int k = 0; k < table.pages.size(); k++) {
				Page pgTemp = Page.pdeserialize(table.pages.get(k));
				for (int j = 0; j < pgTemp.tuples.size(); j++) {
					Tuple tplTemp = Tuple.Tpldeserialize((String) pgTemp.tuples
							.get(j));
					System.out.println(tplTemp.toString() + "[" + k + " , " + j
							+ "]");
				}
			}
			table.tblserialize(strTableName);

		} catch (IOException e) {
			throw new DBAppException("problem in Meta data reader");
		}
	}

	public static int indexPK(String strTableName) throws DBAppException {
		Vector<String> ifPK = getIfPK(strTableName);
		for (int i = 0; i < ifPK.size(); i++) {
			if (ifPK.get(i).toLowerCase().equals("true"))
				return i;
		}
		return -1;
	}

	// order AND -> OR -> XOR
	public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
			String[] strarrOperators) throws DBAppException, IOException {
		Vector<Vector<int[]>> searchRes = new Vector<Vector<int[]>>();
		// setup iterator
		Vector<Tuple> resultTuples = new Vector<Tuple>();
		Iterator results = null;
		// extract info from SQLTerms
		Vector<String> TblNames = new Vector<String>();
		Vector<String> colNames = new Vector<String>();
		Vector<String> operators = new Vector<String>();
		Vector<String> searchKeys = new Vector<String>();
		// get AND,OR,XOR
		Vector<String> bigOperators = new Vector<String>();
		Vector<String> colNamesVec = new Vector<String>();
		Vector<String> colTypes = new Vector<String>();
		int indexV = 0;

		for (int i = 0; i < arrSQLTerms.length; i++) {

			TblNames.add(arrSQLTerms[i]._strTableName);
			colNames.add(arrSQLTerms[i]._strColumnName);
			operators.add(arrSQLTerms[i]._strOperator);
			searchKeys.add((String) "" + arrSQLTerms[i]._objValue);
			if (i < arrSQLTerms.length - 1)
				bigOperators.add(strarrOperators[i]);
		}

		for (int i = 0; i < arrSQLTerms.length; i++) {
			try {
				Table currTbl = Table.Tbldeserialize(TblNames.get(i) + ".ser");
				searchRes.add(currTbl.select_search(searchKeys.get(i),
						getColIndex(TblNames.get(i), colNames.get(i)),
						operators.get(i)));
				if (i != 0) {
					if (!TblNames.get(i).equals(TblNames.get(i - 1)))
						throw new DBAppException(
								"Exception: Selecting on different Tables");
				}
			} catch (IOException e) {
				System.out.println("table not found");
				return (new Vector<int[]>()).iterator();
				// return empty iterator;
			}
		}
		colNamesVec = getCols(TblNames.get(0));
		for (int i = 0; i < colNames.size(); i++) {
			if (!colNamesVec.contains(colNames.get(i))) {
				throw new DBAppException("Invalid Column Name");
			}
		}
		colTypes = getColTypes(TblNames.get(0));

		for (int i = 0; i < searchKeys.size(); i++) {
			indexV = getColIndex(TblNames.get(0), colNames.get(i));
			Comparable temp = fromString(searchKeys.get(i));
			try {
				checkType(temp, colTypes.get(indexV));
			} catch (DBAppException E) {
				throw new DBAppException("Invalid Column Type");
			}
		}

		while (!bigOperators.isEmpty()) {
			Vector<int[]> operand1 = searchRes.get(0);
			Vector<int[]> operand2 = searchRes.get(1);
			if (bigOperators.get(0).equals("AND")) {
				searchRes.set(0, getAnd(operand1, operand2));
			} else if (bigOperators.get(0).equals("OR")) {
				searchRes.set(0, getOR(operand1, operand2));
			} else if (bigOperators.get(0).equals("XOR")) {
				searchRes.set(0, getXOR(operand1, operand2));
			} else {
				throw new DBAppException("invalid select statement");
			}
			searchRes.remove(1);
			bigOperators.remove(0);
		}

		Vector<int[]> finalLoc = searchRes.get(0);
		Table Tbl = Table.Tbldeserialize(TblNames.get(0) + ".ser");
		for (int i = 0; i < finalLoc.size(); i++) {
			Page tmpPage = Page.pdeserialize(Tbl.pages.get(finalLoc.get(i)[0]));
			Tuple tmpTuple = Tuple.Tpldeserialize((String) tmpPage.tuples
					.get(finalLoc.get(i)[1]));
			resultTuples.add(tmpTuple);
		}
		results = resultTuples.iterator();
		return results;
	}

	public static Vector<int[]> getAnd(Vector<int[]> para1, Vector<int[]> para2) {
		Vector<int[]> result = new Vector<int[]>();
		for (int i = 0; i < para1.size(); i++) {
			for (int j = 0; j < para2.size(); j++) {
				if (para1.get(i)[0] == para2.get(j)[0]
						&& para1.get(i)[1] == para2.get(j)[1])
					result.add(para1.get(i));
			}
		}
		return result;
	}

	public static Vector<int[]> getOR(Vector<int[]> para1, Vector<int[]> para2) {
		Vector<int[]> result = new Vector<int[]>();
		int i = 0;
		while (i < para1.size()) {
			for (int j = 0; j < para2.size(); j++) {
				if (para1.get(i)[0] == para2.get(j)[0]
						&& para1.get(i)[1] == para2.get(j)[1]) {
					para1.remove(i);
					i--;
					break;
				}
			}
			i++;
		}
		for (int k = 0; k < para1.size(); k++)
			result.add(para1.get(k));

		for (int k = 0; k < para2.size(); k++)
			result.add(para2.get(k));

		return result;
	}

	public static Vector<int[]> getXOR(Vector<int[]> para1, Vector<int[]> para2) {
		Vector<int[]> result = new Vector<int[]>();
		int i = 0;
		while (i < para1.size()) {
			int j = 0;
			while (j < para2.size()) {
				if (para1.get(i)[0] == para2.get(j)[0]
						&& para1.get(i)[1] == para2.get(j)[1]) {
					para1.remove(i);
					para2.remove(j);
					j--;
					i--;
					break;
				}
				j++;
			}
			i++;
		}
		for (int k = 0; k < para1.size(); k++)
			result.add(para1.get(k));

		for (int k = 0; k < para2.size(); k++)
			result.add(para2.get(k));

		return result;
	}

	// public static void main (String[] args)throws IOException {
	//
	// Table t = Table.Tbldeserialize("Student.ser");
	// System.out.println(t.pages.size());
	// // Hashtable htblColNameValue = new Hashtable();
	// // htblColNameValue.clear();
	// // htblColNameValue.put("id", new Integer(453455));
	// // htblColNameValue.put("name", new String("Ahmed Noor"));
	// // htblColNameValue.put("gpa", new Double(0.95));
	// //
	// // dbApp.deleteFromTable("Student", (Hashtable)htblColNameValue);
	// // t.pages.clear();
	// // t.tblserialize("Student");
	// // p.tuples.clear();
	// // System.out.println(p.tuples.size());
	// // System.out.println(p.tuples.get(0));
	// // System.out.println(t.pages.size());
	// System.out.println("Number of Pages" + t.pages.size());
	// for (int h = 0; h < t.pages.size(); h++) {
	// Page p = Page.pdeserialize(t.pages.get(h));
	// Tuple tu;
	// for (int i = 0; i < p.tuples.size(); i++) {
	// tu = Tuple.Tpldeserialize(p.tuples.get(i));
	// System.out.println(tu.values.get(0) + " " + tu.values.get(1) + " " +
	// tu.values.get(2));
	// }
	// System.out.println("end of pg");
	// }
	//
	// // System.out.println(t.pages.size());
	// // System.out.println(t.pageMin.size());
	// // for (int i = 0; i < t.pageMin.size(); i++) {
	// // System.out.println(t.pageMin.get(i) + " " + t.pageMax.get(i));
	// // }
	//
	// // Vector<int[]> v = t.search("0.95",);
	// System.out.println("\n \n");
	// for (int i = 0; i < v.size(); i++) {
	// if (v == null)
	// System.out.println("not found");
	// else {
	// System.out.println(v.get(i)[0] + " " + v.get(i)[1]);
	// }
	// }
	//
	// // Vector<int[]> v = t.search("Ahmed Noor", 1);
	// // if (v.isEmpty())
	// // System.out.println("search is not successfull");
	// // for (int i = 0; i < v.size(); i++) {
	// // int[] a = v.get(i);
	// // System.out.print(a[0] + "," + a[1] + " ");
	// // }
	// // System.out.println();
	// //
	// // t.binary_search("1.5", 0);
	// //
	// }

	public static void main(String[] args) {
		try {
			Table t = new Table("Student");
			String strTableName = "Student";
			String strTableName1 = "Parent";
			String strClusteringKeyValue = "id";
			String strClusteringKeyValue1 = "name";
			DBApp dbApp = new DBApp();
			int N = DBApp.readNumberFromConfig();
			System.out.println(N);

			Hashtable htblColNameType = new Hashtable();
			Hashtable htblColNameType3 = new Hashtable();
			Hashtable htblColNameValue = new Hashtable();
			Hashtable htblColNameType2 = new Hashtable();

//			 htblColNameType.put("id", "java.lang.Integer");
//			 htblColNameType.put("name", "java.lang.String");
//			 htblColNameType.put("gpa", "java.lang.double");
//			 htblColNameType2.put("id", "java.lang.Integer");
//			 htblColNameType2.put("name", "java.lang.String");
//			 htblColNameType2.put("gpa", "java.lang.double");
//			
//			 dbApp.createTable(strTableName, "id", htblColNameType);
//			 dbApp.createTable(strTableName1, "name", htblColNameType2);
//			 dbApp.createIndex(strTableName, "gpa", "gpaIndex");
//			 htblColNameValue.clear();
//			 htblColNameValue.put("id", new Integer(1));
//			 htblColNameValue.put("name", new String("Roaa"));
//			 htblColNameValue.put("gpa", new Double(0.7));
//			 dbApp.insertIntoTable(strTableName, htblColNameValue);
//			
//			 htblColNameValue.clear();
//			 htblColNameValue.put("id", new Integer(7));
//			 htblColNameValue.put("name", new String("Dalia Noor"));
//			 htblColNameValue.put("gpa", new Double(1.25));
//			 dbApp.insertIntoTable(strTableName, htblColNameValue);
//			
//			 htblColNameValue.clear();
//			 htblColNameValue.put("id", new Integer(4));
//			 htblColNameValue.put("name", new String("menna ss"));
//			 htblColNameValue.put("gpa", new Double(0.8));
//			 dbApp.insertIntoTable(strTableName, htblColNameValue);
//			
//			 htblColNameValue.clear();
//			 htblColNameValue.put("id", new Integer(3));
//			 htblColNameValue.put("name", new String("Mohy Ahmed"));
//			 htblColNameValue.put("gpa", new Double(1.7));
//			 dbApp.insertIntoTable(strTableName, htblColNameValue);
//			
//			 htblColNameValue.clear();
//			 htblColNameValue.put("id", new Integer(5));
//			 htblColNameValue.put("name", new String("Jana Noor"));
//			 htblColNameValue.put("gpa", new Double(0.95));
//			 // dbApp.deleteFromTable(strTableName, htblColNameValue);
//			 dbApp.insertIntoTable(strTableName, htblColNameValue);
//			
//			 htblColNameValue.clear();
//			 htblColNameValue.put("id", new Integer(2));
//			 htblColNameValue.put("name", new String("Monica Maged"));
//			 htblColNameValue.put("gpa", new Double(0.95));
//			 // dbApp.deleteFromTable(strTableName, htblColNameValue);
//			 dbApp.insertIntoTable(strTableName, htblColNameValue);
//			
//			 htblColNameValue.clear();
//			 htblColNameValue.put("id", new Integer(13));
//			 htblColNameValue.put("name", new String("Mohamed Samir"));
//			 htblColNameValue.put("gpa", new Double(0.99));
//			 // dbApp.updateTable(strTableName, "11" , htblColNameValue);
//			 dbApp.insertIntoTable(strTableName, htblColNameValue);
//			
//			 htblColNameValue.clear();
//			 htblColNameValue.put("id", new Integer(10));
//			 htblColNameValue.put("name", new String("Zaky Noor"));
//			 htblColNameValue.put("gpa", new Double(1.7));
//			 dbApp.insertIntoTable(strTableName, htblColNameValue);
//			
//			 htblColNameValue.put("id", new Integer(15));
//			 htblColNameValue.put("name", new String("steven Samir"));
//			 htblColNameValue.put("gpa", new Double(0.96));
//			 dbApp.deleteFromTable(strTableName, htblColNameValue);
//			 dbApp.insertIntoTable(strTableName, htblColNameValue);
			
//			 htblColNameValue.clear();
//			 htblColNameValue.put("id", new Integer(1));
//			 htblColNameValue.put("name", new String("Roaa "));
//			 htblColNameValue.put("gpa", new Double(0.3));
//			 dbApp.insertIntoTable(strTableName, htblColNameValue);

			// dbApp.deleteFromTable(strTableName, htblColNameValue);
			// // htblColNameValue.clear();
			//
			// // htblColNameValue = new Hashtable();
			// // htblColNameValue.clear();
			// // // htblColNameValue.put("id", new Integer(1));
			// // htblColNameValue.put("name", new String("Zaky Noor"));
			// // // htblColNameValue.put("gpa", new Double(0.95));
			// // dbApp.deleteFromTable(strTableName, htblColNameValue);
			// // htblColNameValue.clear();
			//
			// // htblColNameValue.clear();
			// // htblColNameValue.put("id", new Integer(1));
			// // htblColNameValue.put("name", new String("Zaky Noor"));
			// // htblColNameValue.put("gpa", new Double(0.74));
			// // dbApp.deleteFromTable(strTableName, htblColNameValue);
			// //
			// // htblColNameValue.clear();
			// // htblColNameValue.put("id", new Integer(7));
			// // // htblColNameValue.put("name", new String("Zaky Noor"));
			// // // htblColNameValue.put("gpa", new Double(0.95));
			// // dbApp.deleteFromTable(strTableName, htblColNameValue);
			//
			// // htblColNameValue.clear();
			// // // htblColNameValue.put("id", new Integer(1));
			// // htblColNameValue.put("name", new String("Dalia Noor"));
			// // // htblColNameValue.put("gpa", new Double(0.95));
			// // dbApp.deleteFromTable(strTableName, htblColNameValue);
			// //
			// // htblColNameValue.clear();
			// // // htblColNameValue.put("id", new Integer(1));
			// // htblColNameValue.put("name", new String("monica maged"));
			// // // htblColNameValue.put("gpa", new Double(0.95));
			// // dbApp.deleteFromTable(strTableName, htblColNameValue);
			// htblColNameValue.clear();

			Table table = Table.Tbldeserialize("Student.ser");
			System.out.println("Number of Pages" + table.pages.size());
			for (int h = 0; h < table.pages.size(); h++) {
				Page p = Page.pdeserialize(table.pages.get(h));
				Tuple tu;
				System.out.println("printing page " + h);
				for (int i = 0; i < p.tuples.size(); i++) {
					tu = Tuple.Tpldeserialize((String) p.tuples.get(i));
					System.out.println(tu);
				}
				System.out.println("end of page");
			}
			System.out.println("end of printing");
			System.out.println(table.binary_search("1", 2));
			// htblColNameValue.put("id", new Integer(1));
			// htblColNameValue.put("name", new String("Monica Maged"));
			// htblColNameValue.put("gpa", new Double(0.95));

			// dbApp.deleteFromTable("Student", (Hashtable) htblColNameValue);
			// BTree test=BTree.deserialize("gpaIndex.ser");
			// Vector <int[]> b=(Vector<int[]>) test.search(1.25);
			// for(int u=0;u<b.size();u++)
			// System.out.println("Locations of 1.25=[ "+b.get(u)[0]+" , "+b.get(u)[0]+"
			// ]");
			//
			// System.out.println(test.search(0.95));

			// htblColNameValue.clear();
			// //htblColNameValue.put("id", new Integer(1));
			// htblColNameValue.put("name", new String("Mohamed Samir"));
			// //htblColNameValue.put("gpa", new Double(0.88));
			// dbApp.deleteFromTable("Student", (Hashtable) htblColNameValue);
			// dbApp.updateTable(strTableName, strClusteringKeyValue,
			// htblColNameValue);
			// Hashtable htblColNameValue2 = new Hashtable();
			// // htblColNameValue2.put("id", new Integer(999999990));
			// htblColNameValue2.put("name", new String("Hamed Hamdy"));
			// htblColNameValue2.put("gpa", new Double(0.98));

			// htblColNameValue2.clear();
			// htblColNameValue2.put("id", new Integer(999999990));
			// dbApp.updateTable(strTableName, "1" , htblColNameValue2);;

			SQLTerm[] arrSQLTerms;
			arrSQLTerms = new SQLTerm[1];
			arrSQLTerms[0] = new SQLTerm();
			arrSQLTerms[0]._strTableName = "Student";
			arrSQLTerms[0]._strColumnName = "gpa";
			arrSQLTerms[0]._strOperator = "<";
			arrSQLTerms[0]._objValue = new Double(1.0);
//
//			arrSQLTerms[1] = new SQLTerm();
//			arrSQLTerms[1]._strTableName = "Parent";
//			arrSQLTerms[1]._strColumnName = "id";
//			arrSQLTerms[1]._strOperator = ">=";
//			arrSQLTerms[1]._objValue = new Integer(5);
//
////			arrSQLTerms[2] = new SQLTerm();
////			arrSQLTerms[2]._strTableName = "Student";
////			arrSQLTerms[2]._strColumnName = "id";
////			arrSQLTerms[2]._strOperator = "<=";
////			arrSQLTerms[2]._objValue = new Integer(3343432);
//
			String[] strarrOperators = new String[0];
//			strarrOperators[0] = "AND";
//			//strarrOperators[1] = "AND";
//			// select * from Student where name = "John Noor" or gpa = 1.5;
			Iterator resultSet = dbApp.selectFromTable(arrSQLTerms,
					strarrOperators);

			while (resultSet.hasNext()) {
				System.out.println(resultSet.next());
			}
			

			// Table table = Table.Tbldeserialize("Student.ser");
			// for (int h = 0; h < t.pages.size(); h++) {
			// Page p = Page.pdeserialize(t.pages.get(h));
			// Tuple tu;
			// for (int i = 0; i < p.tuples.size(); i++) {
			// tu = Tuple.Tpldeserialize(p.tuples.get(i));
			// System.out.println(tu.values.get(0) + " "
			// + tu.values.get(1) + " " + tu.values.get(2));
			// }
			// System.out.println("end of pg");
			// }
			//
			// System.out.println("Iterator result");
			// while(resultSet.hasNext()){
			// System.out.println(resultSet.next());
			// }
		} catch (Exception exp) {
			System.out.println("Error here in main method!!!");
			exp.printStackTrace();
		}
	}
	public static void printTable(String StrTableName) throws DBAppException{
		Table table = Table.Tbldeserialize(StrTableName+".ser");
		System.out.println("Number of Pages" + table.pages.size());
		for (int h = 0; h < table.pages.size(); h++) {
			Page p = Page.pdeserialize(table.pages.get(h));
			Tuple tu;
			System.out.println("printing page " + h);
			for (int i = 0; i < p.tuples.size(); i++) {
				tu = Tuple.Tpldeserialize((String) p.tuples.get(i));
				System.out.println(tu);
			}
			System.out.println("end of page");
		}
		System.out.println("end of printing");
	}
}
