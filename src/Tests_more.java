import static org.junit.Assert.*;
import java.security.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.LinkedBlockingQueue;
import org.junit.After;
import org.junit.Test;

public class Tests_more {
	static int search_score = 0;
	static int insert_score = 0;
	static int delete_score = 0;
	// -------------------------- Search test - 5 points ----------------------
	@After
	public void printScore(){
		System.out.println("Score = " + search_score + " + " + insert_score + " + " + delete_score + " = " + (search_score + insert_score + delete_score));
	}

	@Test
	// 1 point
	public void testSearch1_1() {
		Integer testNumbers[] = new Integer[] {1,2,3};
		BPlusTree<Integer, Integer> tree = new BPlusTree<Integer, Integer>();
		Utils.bulkInsert(tree, testNumbers, testNumbers);
		assertEquals(((Integer) 2), tree.search(2));
		search_score += 1;
	}

	@Test
	// 1 point
	public void testSearch2_1() {
		Integer testNumbers[] = new Integer[] {1,2,3,4,5};
		BPlusTree<Integer, Integer> tree = new BPlusTree<Integer, Integer>();
		Utils.bulkInsert(tree, testNumbers, testNumbers);
		assertEquals(((Integer) 3), tree.search(3));
		search_score += 1;
	}

	@Test
	// 1 point
	// Testing values we can really only compare - no cheating!
	public void testSearch3_1() {
		BPlusTree<OnlyComparable<String>, OnlyComparable<Integer>> tree = new BPlusTree<OnlyComparable<String>, OnlyComparable<Integer>>();
		for (int i = 0; i < 1000; i++)
			tree.insert((new OnlyComparable<String>(hash("" + i))),
					(new OnlyComparable<Integer>(((Integer) i))));
		assertEquals(tree.search(new OnlyComparable<String>(hash("" + 625))),
				new OnlyComparable<Integer>((Integer) 625));
		search_score += 1;
	}

	@Test
	// 2 point
	// Do you really get every value we put in?
	public void testSearch4_2() {
		BPlusTree<OnlyComparable<String>, OnlyComparable<Integer>> tree = new BPlusTree<OnlyComparable<String>, OnlyComparable<Integer>>();
		for (int i = 10000; i < 20000; i++)
			tree.insert((new OnlyComparable<String>(hash("" + i))),
					(new OnlyComparable<Integer>(((Integer) i))));
		for (int i = 10000; i < 20000; i++)
			assertEquals(tree.search(new OnlyComparable<String>(hash("" + i))),
					new OnlyComparable<Integer>((Integer) i));
		search_score += 2;
	}

	// -------------------------- Insert test - 15 points ----------------------
	@Test
	// 1 point
	public void testInsertNormal_1() {
		Integer testNumbers[] = new Integer[] {1, 2, 4, 5, 6};
		BPlusTree<Integer, Integer> tree = new BPlusTree<Integer, Integer>();
		Utils.bulkInsert(tree, testNumbers, testNumbers);
	
		tree.insert(3, 3);
		String test = outputTree(tree);
		String result = "@4/@%%[(1,1);(2,2);(3,3);]#[(4,4);(5,5);(6,6);]$%%";
		assertEquals(result, test);
		insert_score += 1;
	}

	@Test
	// 2 point
	// ensure that we can insert stuff based only on comparison operations
	// also, duplicate values
	public void testInsertManyOnlyComparable_2() {
		String poem[] = new String[] { "my", "mother", "said", "to", "pick",
				"the", "very", "best", "one", "and", "you", "are", "it" };
		BPlusTree<OnlyComparable<String>, String> tree = new BPlusTree<OnlyComparable<String>, String>();
		for (int i = 0; i < 10; i++)
			tree.insert((new OnlyComparable<String>(hash("" + i))), (poem[i
					% (poem.length)]));
		String test = outputTree(tree);
		String result = "@OnlyComparable-8F14E45FCEEA167A5A36DEDD4BEA2543/OnlyCompar"
				+ "able-C81E728D9D4C2F636F067F89CC14862C/OnlyComparable-CFCD208495D565EF66"
				+ "E7DFF9F98764DA/@%%[(OnlyComparable-1679091C5A880FAF6FB5E6087EB1B2DC,ver"
				+ "y);(OnlyComparable-45C48CCE2E2D7FBDEA1AFC51C7C6AD26,and);]#[(OnlyCompar"
				+ "able-8F14E45FCEEA167A5A36DEDD4BEA2543,best);(OnlyComparable-A87FF679A2F"
				+ "3E71D9181A67B7542122C,pick);(OnlyComparable-C4CA4238A0B923820DCC509A6F7"
				+ "5849B,mother);]#[(OnlyComparable-C81E728D9D4C2F636F067F89CC14862C,said)"
				+ ";(OnlyComparable-C9F0F895FB98AB9159F51FD0297E236D,one);]#[(OnlyComparab"
				+ "le-CFCD208495D565EF66E7DFF9F98764DA,my);(OnlyComparable-E4DA3B7FBBCE234"
				+ "5D7772B0674A318D5,the);(OnlyComparable-ECCBC87E4B5CE2FE28308FD9F2A7BAF3"
				+ ",to);]$%%";
		assertEquals(result, test);
		insert_score += 2;
	}

	@Test
	// 3 points
	public void testInsertLeafNodeSplit_3() {
		Integer testNumbers[] = new Integer[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
		BPlusTree<Integer, Integer> tree = new BPlusTree<Integer, Integer>();
		Utils.bulkInsert(tree, testNumbers, testNumbers);
	
		String test = outputTree(tree);
		
		String result = "@3/5/7/@%%[(1,1);(2,2);]#[(3,3);(4,4);]#[(5,5);(6,6);]#[(7,7);(8,8);(9,9);(10,10);]$%%";
		assertEquals(result, test);
		insert_score += 3;
	}

	@Test
	// 3 points
	public void testInsertIndexNodeSplit_3() {
		Integer testNumbers[] = new Integer[] { 1, 2, 3, 9, 10, 11, 12, 13, 4, 5, 6, 7, 8};
		BPlusTree<Integer, Integer> tree = new BPlusTree<Integer, Integer>();
		Utils.bulkInsert(tree, testNumbers, testNumbers);
	
		String test = outputTree(tree);
		String result = "@3/5/7/10/@%%[(1,1);(2,2);]#[(3,3);(4,4);]#[(5,5);(6,6);]#[(7,7);(8,8);(9,9);]#[(10,10);(11,11);(12,12);(13,13);]$%%";
		assertEquals(result, test);
		insert_score += 3;
	}

	@Test
	// 3 points
	public void testInsertRootNodeSplit_3() {
		Integer testNumbers[] = new Integer[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
				13, 14, 15, 16, 40, 41, 42, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35};
		BPlusTree<Integer, Integer> tree = new BPlusTree<Integer, Integer>();
		Utils.bulkInsert(tree, testNumbers, testNumbers);
		String test = outputTree(tree);
		String result = "@19/@%%@7/13/@@25/31/@%%@3/5/@@9/11/@@15/17/@@21/23/@@27/29/@@33/40/@%%[(1,1);(2,2);]#[(3,3);(4,4);]#[(5,5);(6,6);]$[(7,7);(8,8);]#[(9,9);(10,10);]#[(11,11);(12,12);]$[(13,13);(14,14);]#[(15,15);(16,16);]#[(17,17);(18,18);]$[(19,19);(20,20);]#[(21,21);(22,22);]#[(23,23);(24,24);]$[(25,25);(26,26);]#[(27,27);(28,28);]#[(29,29);(30,30);]$[(31,31);(32,32);]#[(33,33);(34,34);(35,35);]#[(40,40);(41,41);(42,42);]$%%";
		assertEquals(result, test);
		insert_score += 3;
	}

	@Test
	// 3 points
	// Test that a large tree maintains the appropriate invariants, and builds
	// an efficient structure.
	public void testInsertLargeTree_3() {
		BPlusTree<Integer, Integer> tree = new BPlusTree<Integer, Integer>();
		ArrayList<Integer> numbers = new ArrayList<Integer>(100000);
		for (int i = 0; i < 100000; i++) {
			numbers.add(i);
		}
		Collections.shuffle(numbers);
		for (int i = 0; i < 100000; i++) {
			tree.insert(numbers.get(i), numbers.get(i));
		}
		testTreeInvariants(tree);
		assertTrue(treeDepth(tree.root) < 11);
		insert_score += 3;
	}
	
	
	// -------------------------- Delete test - 25 points ----------------------

	@Test
	// 2 points - normal delete from a leaf node
	public void testDeleteNormal_2() {
		Integer testNumbers[] = new Integer[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
		BPlusTree<Integer, Integer> tree = new BPlusTree<Integer, Integer>();
		Utils.bulkInsert(tree, testNumbers, testNumbers);

		tree.delete(10);
		String test = outputTree(tree);
		String result = "@3/5/7/@%%[(1,1);(2,2);]#[(3,3);(4,4);]#[(5,5);(6,6);]#[(7,7);(8,8);(9,9);]$%%";
		assertEquals(result, test);
		delete_score += 2;
	}

	// testing proper leaf node deletion - redistribute behavior with left sibling
	// 2 points
	@Test
	public void testDeleteLeafNodeRedistributeLeft_2() {
		Integer testNumbers[] = new Integer[] { 1, 3, 6, 7, 4, 5, 2 };
		String testNumberStrings[] = new String[testNumbers.length];
		for (int i = 0; i < testNumbers.length; i++) {
			testNumberStrings[i] = (testNumbers[i]).toString();
		}
		BPlusTree<Integer, String> tree = new BPlusTree<Integer, String>();
		Utils.bulkInsert(tree, testNumbers, testNumberStrings);
		

		tree.delete(5);
		tree.delete(6);
		tree.delete(7);
		String test = outputTree(tree);
		String result = "@3/@%%[(1,1);(2,2);]#[(3,3);(4,4);]$%%";
		assertEquals(result, test);
		delete_score += 2;
	}

	@Test
	// testing proper leaf node deletion - redistribute behavior with right sibling
	// 2 points
	public void testDeleteLeafNodeRedistributeRight_2() {
		Integer testNumbers[] = new Integer[] { 2, 7, 8, 6, 10, 11, 1, 3, 4, 5 };
		BPlusTree<Integer, Integer> tree = new BPlusTree<Integer, Integer>();
		Utils.bulkInsert(tree, testNumbers, testNumbers);
		
		tree.delete(2);
	
		String test = outputTree(tree);
		String result = "@4/7/@%%[(1,1);(3,3);]#[(4,4);(5,5);(6,6);]#[(7,7);(8,8);(10,10);(11,11);]$%%";
		assertEquals(result, test);
		delete_score += 2;
	}

	@Test
	// testing proper leaf node deletion - merge behavior with left sibling
	// 2 points
	public void testDeleteLeafNodeMergeLeft_2() {
		Integer testNumbers[] = new Integer[] { 1, 2, 7, 8, 5, 6, 10, 11};
		BPlusTree<Integer, Integer> tree = new BPlusTree<Integer, Integer>();
		Utils.bulkInsert(tree, testNumbers, testNumbers);
		tree.delete(6);

		String test = outputTree(tree);

		String result = "@7/@%%[(1,1);(2,2);(5,5);]#[(7,7);(8,8);(10,10);(11,11);]$%%";
		assertEquals(result, test);
		delete_score += 2;
	}

	@Test
	// testing proper leaf node deletion - merge behavior with right sibling
	// 2 points
	public void testDeleteLeafNodeMergeRight_2() {
		Integer testNumbers[] = new Integer[] { 1, 2, 7, 8, 5, 6, 10, 11};
		BPlusTree<Integer, Integer> tree = new BPlusTree<Integer, Integer>();
		Utils.bulkInsert(tree, testNumbers, testNumbers);

		tree.delete(2);

		String test = outputTree(tree);

		String result = "@7/@%%[(1,1);(5,5);(6,6);]#[(7,7);(8,8);(10,10);(11,11);]$%%";
		assertEquals(result, test);
		delete_score += 2;
	}
	
	@Test
	// testing deletion - index node merges with left sibling
	// 2 points
	public void testDeleteIndexNodeMergeLeft_2() {
		Integer testNumbers[] = new Integer[] { 2, 4, 5, 7, 8, 9, 10, 11, 12,
				13, 14, 15, 16 };
		BPlusTree<Integer, Integer> tree = new BPlusTree<Integer, Integer>();
		Utils.bulkInsert(tree, testNumbers, testNumbers);
		outputTree(tree);
		tree.delete(16);
		tree.delete(15);
		String test = outputTree(tree);

		String result = "@5/8/10/12/@%%[(2,2);(4,4);]#[(5,5);(7,7);]#[(8,8);(9,9);]#[(10,10);(11,11);]#[(12,12);(13,13);(14,14);]$%%";
		assertEquals(result, test);
		delete_score += 2;
	}

	@Test
	// testing deletion - index node merges with right sibling
	// 2 points
	public void testDeleteIndexNodeMergeRight_2() {
		Integer testNumbers[] = new Integer[] { 2, 4, 5, 7, 8, 9, 10, 11, 12,
				13, 14, 15, 16};
		BPlusTree<Integer, Integer> tree = new BPlusTree<Integer, Integer>();
		Utils.bulkInsert(tree, testNumbers, testNumbers);
		outputTree(tree);
		tree.delete(7);
		String test = outputTree(tree);
		String result = "@8/10/12/14/@%%[(2,2);(4,4);(5,5);]#[(8,8);(9,9);]#[(10,10);(11,11);]#[(12,12);(13,13);]#[(14,14);(15,15);(16,16);]$%%";
		assertEquals(result, test);
		delete_score += 2;
	}

	@Test
		// testing deletion - index node redistributes with left sibling
		// 2 points
		public void testDeleteIndexNodeRedistributeLeft_2() {
			Integer testNumbers[] = new Integer[] { 1, 5, 32, 30, 28, 27, 26, 
					 25, 23, 22, 16, 15, 10, 13, 14, 12, 9, 8, 2};
			BPlusTree<Integer, Integer> tree = new BPlusTree<Integer, Integer>();
			Utils.bulkInsert(tree, testNumbers, testNumbers);
			tree.delete(26);
			tree.delete(27);
			tree.delete(22);;
			String test = outputTree(tree);
			String result = "@13/@%%@5/10/@@16/28/@%%[(1,1);(2,2);]#[(5,5);(8,8);(9,9);]#[(10,10);(12,12);]$[(13,13);(14,14);(15,15);]#[(16,16);(23,23);(25,25);]#[(28,28);(30,30);(32,32);]$%%";
			assertEquals(result, test);
			delete_score += 2;
		}

	@Test
	// testing deletion - index node redistributes with right sibling
	// 2 points
	public void testDeleteIndexNodeRedistributeRight_2() {
		Integer testNumbers[] = new Integer[] { 2, 4, 5, 7, 8, 9, 10, 11, 12,
				13, 14, 15, 16, 17, 18 };
		BPlusTree<Integer, Integer> tree = new BPlusTree<Integer, Integer>();
		Utils.bulkInsert(tree, testNumbers, testNumbers);

		tree.delete(7);
		String test = outputTree(tree);
		String result = "@12/@%%@8/10/@@14/16/@%%[(2,2);(4,4);(5,5);]#[(8,8);(9,9);]#[(10,10);(11,11);]$[(12,12);(13,13);]#[(14,14);(15,15);]#[(16,16);(17,17);(18,18);]$%%";
		assertEquals(result, test);
		delete_score += 2;
	}
	
	@Test
	// 2 points
	// Test that a large tree maintains the appropriate invariants, and builds
	// an efficient structure, even after deletes.
	public void testLargeTreeDelete_2() {
		BPlusTree<Integer, Integer> tree = new BPlusTree<Integer, Integer>();
		ArrayList<Integer> numbers = new ArrayList<Integer>(100000);
		for (int i = 0; i < 100000; i++) {
			numbers.add(i);
		}
		Collections.shuffle(numbers);
		for (int i = 0; i < 100000; i++) {
			tree.insert(numbers.get(i), numbers.get(i));
		}
		Collections.shuffle(numbers);
		for (int i = 0; i < 80000; i++) {
			tree.delete(numbers.get(i));
		}
		
		if (!tree.root.isLeafNode) {
			testTreeInvariants(tree);
		}
		assertTrue(treeDepth(tree.root) < 10);
		delete_score += 2;
	}
	
	// 1 points
	// add some nodes, delete a few and check
	@Test
	public void testSimpleHybrid2_1() {
		Character alphabet[] = new Character[] { 'a', 'b', 'c', 'd', 'e', 'f',
				'g', 'h', 'i', 'j', 'k'};
		String alphabetStrings[] = new String[alphabet.length];
		for (int i = 0; i < alphabet.length; i++) {
			alphabetStrings[i] = (alphabet[i]).toString();
		}
		BPlusTree<Character, String> tree = new BPlusTree<Character, String>();
		Utils.bulkInsert(tree, alphabet, alphabetStrings);
		String test = outputTree(tree);
		String correct = "@c/e/g/i/@%%[(a,a);(b,b);]#[(c,c);(d,d);]#[(e,e);(f,f);]#[(g,g);(h,h);]#[(i,i);(j,j);(k,k);]$%%";
	
		assertEquals(correct, test);
	
		tree.delete('a');
		tree.delete('e');
		tree.delete('g');
		test = outputTree(tree);
		correct = "@d/i/@%%[(b,b);(c,c);]#[(d,d);(f,f);(h,h);]#[(i,i);(j,j);(k,k);]$%%";
		assertEquals(correct, test);
		delete_score += 1;
	}

	// 2 points
	// add some nodes, see if it comes out right, delete one, see if it's right
	@Test
	public void testHybridLarge_2() {
		BPlusTree<Integer, Integer> tree = new BPlusTree<Integer, Integer>();
		ArrayList<Integer> numbers = new ArrayList<Integer>(100003);
		Integer num = 1;
		for (int i = 0; i < 100002; i++) {
			numbers.add(num);
			num = (num * 7)%100003;
		}
		for (int i = 0; i < 100002; i++) {
			tree.insert(numbers.get(i), numbers.get(i));
		}
		for (int i = 0; i < 100003; i++) {
			if(i%10000 !=0){
			tree.delete(i);
			}
		}
		String test = outputTree(tree);
		String correct = "@30000/50000/70000/90000/@%%[(10000,10000);(20000,20000);]#[(30000,30000);(40000,40000);]#[(50000,50000);(60000,60000);]#[(70000,70000);(80000,80000);]#[(90000,90000);(100000,100000);]$%%";
		assertEquals(correct, test);
		delete_score += 2;
	}
	
	
	@Test
	// 2 points
	public void testBookExampleLong_2() {
		Integer exampleNumbers[] = new Integer[] { 2, 3, 13, 14, 17, 19, 24, 27,
				30, 33, 34, 38, 5, 7, 16, 20, 22, 29 };
		String primeNumberStrings[] = new String[exampleNumbers.length];
		for (int i = 0; i < exampleNumbers.length; i++) {
			primeNumberStrings[i] = (exampleNumbers[i]).toString();
		}
		BPlusTree<Integer, String> tree = new BPlusTree<Integer, String>();
		Utils.bulkInsert(tree, exampleNumbers, primeNumberStrings);

		tree.delete(13);
		tree.delete(17);
		tree.delete(30);
		tree.insert(39, "39");
		// Initial tree
		String test = Utils.outputTree(tree);
		String correct = "@13/17/24/30/@%%[(2,2);(3,3);(5,5);(7,7);]#[(14,14);(16,16);]#[(19,19);(20,20);(22,22);]#[(24,24);(27,27);(29,29);]#[(33,33);(34,34);(38,38);(39,39);]$%%";
		assertEquals(test, correct);

		// Insert 8
		tree.insert(8, "8");
		test = Utils.outputTree(tree);
		correct = "@17/@%%@5/13/@@24/30/@%%[(2,2);(3,3);]#[(5,5);(7,7);(8,8);]#[(14,14);(16,16);]$[(19,19);(20,20);(22,22);]#[(24,24);(27,27);(29,29);]#[(33,33);(34,34);(38,38);(39,39);]$%%";
		
		assertEquals(test, correct);

		// Delete 19 and 20
		tree.delete(19);
		tree.delete(20);
		
		test = Utils.outputTree(tree);
		correct = "@17/@%%@5/13/@@27/30/@%%[(2,2);(3,3);]#[(5,5);(7,7);(8,8);]#[(14,14);(16,16);]$[(22,22);(24,24);]#[(27,27);(29,29);]#[(33,33);(34,34);(38,38);(39,39);]$%%";
		assertEquals(test, correct);

		// Delete 24
		tree.delete(24);
		
		test = Utils.outputTree(tree);
		correct = "@5/13/17/30/@%%[(2,2);(3,3);]#[(5,5);(7,7);(8,8);]#[(14,14);(16,16);]#[(22,22);(27,27);(29,29);]#[(33,33);(34,34);(38,38);(39,39);]$%%";
		assertEquals(test, correct);
		delete_score += 2;
	}
	
	public <K extends Comparable<K>, T> int treeDepth(Node<K, T> node) {
		if (node.isLeafNode)
			return 1;
		int childDepth = 0;
		int maxDepth = 0;
		for (Node<K, T> child : ((IndexNode<K, T>) node).children) {
			childDepth = treeDepth(child);
			if (childDepth > maxDepth)
				maxDepth = childDepth;
		}
		return (1 + maxDepth);
	}

	/**
	 * hash returns a hex MD5 hash of a string. If weird exceptions happen, it
	 * returns the string itself.
	 * 
	 * @param s
	 *            - string to be hashed
	 * @return a hex MD5 hash of s
	 */
	public String hash(String s) {
		try {
			return javax.xml.bind.DatatypeConverter
					.printHexBinary((MessageDigest.getInstance("MD5")).digest(s
							.getBytes("UTF-8")));
		} catch (Exception e) {
			return s;
		}
	}

	private class OnlyComparable<T extends Comparable<T>> implements
			Comparable<OnlyComparable<T>> {
		public T v;

		public OnlyComparable(T value) {
			v = value;
		}

		public int compareTo(OnlyComparable<T> other) {
			return v.compareTo(other.v);
		}

		public String toString() {
			return ("OnlyComparable-" + v.toString());
		}

		public boolean equals(Object other) {
			return (toString().equals(other.toString()));
		}
	}

	public static <K extends Comparable<K>, T> String outputTree(
			BPlusTree<K, T> tree) {
		/* Temporary queue. */
		LinkedBlockingQueue<Node<K, T>> queue;

		/* Create a queue to hold node pointers. */
		queue = new LinkedBlockingQueue<Node<K, T>>();
		String result = "";

		int nodesInCurrentLevel = 1;
		int nodesInNextLevel = 0;
		ArrayList<Integer> childrenPerIndex = new ArrayList<Integer>();
		queue.add(tree.root);
		while (!queue.isEmpty()) {
			Node<K, T> target = queue.poll();
			nodesInCurrentLevel--;
			if (target.isLeafNode) {
				LeafNode<K, T> leaf = (LeafNode<K, T>) target;
				result += "[";
				for (int i = 0; i < leaf.keys.size(); i++) {
					result += "(" + leaf.keys.get(i) + "," + leaf.values.get(i)
							+ ");";
				}
				if (childrenPerIndex.isEmpty()) {
					result += "]$";
				} else {
					childrenPerIndex.set(0, childrenPerIndex.get(0) - 1);
					if (childrenPerIndex.get(0) == 0) {
						result += "]$";
						childrenPerIndex.remove(0);
					} else {
						result += "]#";
					}

				}
			} else {
				IndexNode<K, T> index = ((IndexNode<K, T>) target);
				result += "@";
				for (int i = 0; i < index.keys.size(); i++) {
					result += "" + index.keys.get(i) + "/";
				}
				result += "@";
				queue.addAll(index.children);
				if (index.children.get(0).isLeafNode) {
					childrenPerIndex.add(index.children.size());
				}
				nodesInNextLevel += index.children.size();
			}

			if (nodesInCurrentLevel == 0) {
				result += "%%";
				nodesInCurrentLevel = nodesInNextLevel;
				nodesInNextLevel = 0;
			}

		}

		return result;

	}

	public <K extends Comparable<K>, T> void testNodeInvariants(Node<K, T> node) {
		if (node.keys.size() < BPlusTree.D
				|| node.keys.size() > 2 * BPlusTree.D) {
			System.out.println("#keys: " + node.keys.size());
			System.out.println("Error");
		}
		assertFalse(node.keys.size() > 2 * BPlusTree.D);
		assertFalse(node.keys.size() < BPlusTree.D);
		if (!(node.isLeafNode))
			for (Node<K, T> child : ((IndexNode<K, T>) node).children)
				testNodeInvariants(child);
	}

	public <K extends Comparable<K>, T> void testTreeInvariants(
			BPlusTree<K, T> tree) {
		for (Node<K, T> child : ((IndexNode<K, T>) (tree.root)).children)
			testNodeInvariants(child);
	}
}
