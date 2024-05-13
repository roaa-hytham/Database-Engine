package BTree_pkge;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Vector;

import BTree_pkge.BTreeLeafNode;
import database_project.Table;

/**
 * A B+ tree Since the structures and behaviors between internal node and
 * external node are different, so there are two different classes for each kind
 * of node.
 *
 * @param < TKey >
 *            the data type of the key
 * @param < TValue >
 *            the data type of the value
 */
public class BTree<TKey extends Comparable<TKey>, TValue> implements
		Serializable {
	/**
	 * @uml.property name="root"
	 * @uml.associationEnd multiplicity="(1 1)"
	 */
	private BTreeNode<TKey> root;
	/**
	 * @uml.property name="tableName"
	 */
	private String tableName;

	public BTree() {
		this.root = new BTreeLeafNode<TKey, TValue>();
	}

	/**
	 * Insert a new key and its associated value into the B+ tree.
	 */
	public void insert(TKey key, TValue value) {
		BTreeLeafNode<TKey, TValue> leaf = this
				.findLeafNodeShouldContainKey(key);
		leaf.insertKey(key, value);

		if (leaf.isOverflow()) {
			BTreeNode<TKey> n = leaf.dealOverflow();
			if (n != null)
				this.root = n;
		}
	}

	/**
	 * Search a key value on the tree and return its associated value.
	 */
	public TValue search(TKey key) {
		BTreeLeafNode<TKey, TValue> leaf = this
				.findLeafNodeShouldContainKey(key);
		TValue empty = (TValue) new Vector<int[]>();
		int index = leaf.search(key);
		return (index == -1) ? empty : leaf.getValue(index);
	}

	public Vector<int[]> searchSmaller(TKey key) {
		Vector<int[]> arrResults = new Vector<int[]>();
		Vector<TValue> results = new Vector<TValue>();
		BTreeLeafNode<TKey, TValue> leaf = getSmallest();
		while (leaf != null) {
			for (int i = 0; i < leaf.getKeyCount(); i++) {
				if (leaf.getKey(i).compareTo(key) >= 0)
					break;
				results.add(leaf.getValue(i));
			}
			leaf = (BTree_pkge.BTreeLeafNode<TKey, TValue>) leaf.rightSibling;
		}
		for(int i=0; i<results.size();i++) {
			Vector<int[]> currTVal = (Vector<int[]>) results.get(i);
			for(int j=0; j<currTVal.size(); j++) {
				arrResults.add(currTVal.get(j));
			}
		}
		
		return arrResults;
	}

	public Vector<int[]> searchGreater(TKey key) {
		Vector<int[]> arrResults = new Vector<int[]>();
		Vector<TValue> results = new Vector<TValue>();
		BTreeLeafNode<TKey, TValue> leaf = getSmallest();
		while (leaf != null) {
			for (int i = 0; i < leaf.getKeyCount(); i++) {
				if (leaf.getKey(i).compareTo(key) > 0)
					results.add(leaf.getValue(i));
			}
			leaf = (BTree_pkge.BTreeLeafNode<TKey, TValue>) leaf.rightSibling;
		}
		for(int i=0; i<results.size();i++) {
			Vector<int[]> currTVal = (Vector<int[]>) results.get(i);
			for(int j=0; j<currTVal.size(); j++) {
				arrResults.add(currTVal.get(j));
			}
		}
		return arrResults;
	}

	/**
	 * Delete a key and its associated value from the tree.
	 */
	public void delete(TKey key) {
		BTreeLeafNode<TKey, TValue> leaf = this
				.findLeafNodeShouldContainKey(key);

		if (leaf.delete(key) && leaf.isUnderflow()) {
			BTreeNode<TKey> n = leaf.dealUnderflow();
			if (n != null)
				this.root = n;
		}
	}

	/**
	 * Search the leaf node which should contain the specified key
	 */
	@SuppressWarnings("unchecked")
	private BTreeLeafNode<TKey, TValue> findLeafNodeShouldContainKey(TKey key) {
		BTreeNode<TKey> node = this.root;
		while (node.getNodeType() == TreeNodeType.InnerNode) {
			node = ((BTreeInnerNode<TKey>) node).getChild(node.search(key));
		}

		return (BTreeLeafNode<TKey, TValue>) node;
	}

	public BTreeLeafNode getSmallest() {
		return this.root.getSmallest();
	}

	public String commit() {
		return this.root.commit();
	}

	public void serialize(String strIndexName) {
		try {
			// Saving of object in a file
			FileOutputStream file = new FileOutputStream(strIndexName + ".ser");
			ObjectOutputStream out = new ObjectOutputStream(file);

			// Method for serialization of object
			out.writeObject(this);
			out.close();
			file.close();
		} catch (IOException ex) {
			ex.printStackTrace();
			System.out.println("IOException is caught");
			System.out.println("Table serialization error!!");
		}
	}

	public static BTree deserialize(String filename) {
		BTree Temp = null;
		try {
			FileInputStream file = new FileInputStream(filename);
			ObjectInputStream in = new ObjectInputStream(file);

			Temp = (BTree) in.readObject();

			in.close();
			file.close();
		} catch (IOException ex) {
			System.out.println("IOException is caught");
			System.out.println("Btree deserialization error!!");
		} catch (ClassNotFoundException ex) {
			System.out.println("ClassNotFoundException is caught");
		}
		return Temp;
	}

	public void print() {
		ArrayList<BTreeNode> upper = new ArrayList<>();
		ArrayList<BTreeNode> lower = new ArrayList<>();
		System.out.println(root + "   Root  before ");

		upper.add(root);
		System.out.println(root + "   Root after ");

		int o = 0;
		System.out.println("Beforeee while");
		while (!upper.isEmpty()) {
			System.out.println("While Start");
			BTreeNode cur = upper.get(0);

			if (cur instanceof BTreeInnerNode) {
				ArrayList<BTreeNode> children = ((BTreeInnerNode) cur)
						.getChildren();
				// System.out.println(cur.toString() + " hi ");

				for (int i = 0; i < children.size(); i++) {
					BTreeNode child = children.get(i);
					System.out.println(child + "   Children");
					if (child != null)
						lower.add(child);
				}
			} else if (cur instanceof BTreeLeafNode) {
				for (int i = 0; i < cur.keys.length; i++) {

					System.out.println(cur.keys[i] + " IN Bleafnode");
				}
				// System.out.println(cur.toString() + " hi ");
			}
			for (int y = 0; y < cur.keys.length; y++)
				System.out.println(cur.keys[y] + " numbers ");

			upper.remove(0);
			if (upper.isEmpty()) {
				System.out.println("\n");
				upper = lower;
				lower = new ArrayList<>();
			}
			o++;
			System.out.println(" END WHILEE ");

		}
	}

	public static void main(String[] args) {
		BTree BT = new BTree();
		Vector<int[]> V = new Vector<int[]>();
		int[] i = { 1, 2 };
		int[] j = { 4, 3 };
		int[] k = { 9, 0 };
		int[] l = { 3, 2 };
		int[] m = { 0, 2 };
		// BT.delete(3);
		V.add(i);
		V.add(j);
		BT.insert(5, V);
		Vector<int[]> V2 = new Vector<int[]>();
		V2.add(k);
		V2.add(l);
		BT.insert(1, V2);
		Vector<int[]> V3 = new Vector<int[]>();
		V3.add(i);
		V3.add(j);
		BT.insert(8, V3);
		Vector<Vector<int[]>> searchS = BT.searchGreater(5);
		for (int f = 0; f < searchS.size(); f++) {
			Vector<int[]> temp = searchS.get(f);
			for (int b = 0; b < temp.size(); b++) {
				System.out.println(temp.get(b)[0] + " ," + temp.get(b)[1]);
			}
			System.out.println("");
		}

	}
}
