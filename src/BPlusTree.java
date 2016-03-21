import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Stack;
import java.util.AbstractMap.SimpleEntry;

/**
 * BPlusTree Class Assumptions: 1. No duplicate keys inserted 2. Order D:
 * D<=number of keys in a node <=2*D 3. All keys are non-negative
 * TODO: Rename to BPlusTree
 */
public class BPlusTree<K extends Comparable<K>, T> {

	public Node<K,T> root;
	public static final int D = 2;
	public boolean isEmpty = true;
	/**
	 * TODO Search the value for a specific key
	 * 
	 * @param key
	 * @return value
	 */
	public T search(K key) {
		// create a nodeInfo object
		NodeInfo nodeInfo = new NodeInfo();
		// search from the root of this tree
		searchLeafNode(key, root, nodeInfo);
		// return null if not find leafNode
		if(nodeInfo.leafNode == null) return null;
		// find the correct position of the key in leafNode.keys
		int keyIndex = Collections.binarySearch(nodeInfo.leafNode.keys, key);
		// if keyIndex >= 0, means position found
		if(keyIndex >= 0) return nodeInfo.leafNode.values.get(keyIndex);
		// if keyIndex < 0, means key doesn't exist in leafNode.keys
		else return null;
	}

	/**
	 * Search the leafNode for a specific key, usually starts from root (recursive method)
	 * 
	 * @param key
	 * @param node
	 * @param nodeInfo
	 */
	public void searchLeafNode(K key, Node<K,T> node, NodeInfo nodeInfo) {
		if(node == null) return;
		// if node is leafNode, record it in nodeInfo
		if(node.isLeafNode) {
			nodeInfo.leafNode = (LeafNode<K,T>)node;
		}
		// else if node is indexNode
		else if(!node.isLeafNode) {
			// record this indexNode in nodePath of nodeInfo
			nodeInfo.nodePath.push((IndexNode<K,T>)node);
			// binary search to find the keyIndex
			int keyIndex = Collections.binarySearch(node.keys, key);
			// if key found in indexNode.keys
			if(keyIndex >= 0) keyIndex += 1;
			// if not found in indexNode.keys, use the insert position the key should be inserted into
			else keyIndex = -(keyIndex + 1);
			// record this keyIndex in indexPath of nodeInfo
			nodeInfo.indexPath.push(keyIndex);
			// recursively call searchLeafNode() method
			searchLeafNode(key, ((IndexNode<K,T>) node).children.get(keyIndex), nodeInfo);
		}
	}

	/**
	 * TODO Insert a key/value pair into the BPlusTree
	 * 
	 * @param key
	 * @param value
	 */
	public void insert(K key, T value) {
		// if this tree is empty, assign the new leafNode to the root
		if(isEmpty) {
			root = new LeafNode<K,T>(key,value);
			isEmpty = false;
		}
		// if this tree isn't empty
		else {
			// create nodeInfo to record info through search
			NodeInfo nodeInfo = new NodeInfo();
			searchLeafNode(key, root, nodeInfo);
			// get the leafNode and nodePath from nodeInfo
			LeafNode<K,T> leafNode = nodeInfo.leafNode;
			Stack<IndexNode<K,T>> nodePath = nodeInfo.nodePath;
			// insert (key, value) pair into this leafNode
			leafNode.insertSorted(key, value);
			// if leafNode overflowed
			if(leafNode.isOverflowed()) {
				// call splitLeafNode, only if the leaf node is overflowed
				Entry<K, Node<K,T>> entry = splitLeafNode(leafNode, nodePath);
				// call splitIndexNode, whether or not the index node is overflowed, this function will handle that
				if(!nodePath.empty()) {
					splitIndexNode(nodePath.pop(), entry, nodePath);
				}
			}
		}
	}

	/**
	 * TODO Split a leaf node and return the new right node and the splitting
	 * key as an Entry<slitingKey, RightNode>
	 * 
	 * @param leaf
	 * @param nodePath
	 * @return the key/node pair as an Entry
	 */
	public Entry<K, Node<K,T>> splitLeafNode(LeafNode<K,T> leaf, Stack<IndexNode<K,T>> nodePath) {
		// store the original keys and values
		ArrayList<K> originKeys = leaf.keys;
		ArrayList<T> originValues = leaf.values;
		// take a look at the view of the original keys' and values' subList from 0 to D-1
		// copy the first D keys and values, then keep them at the original leaf node
		leaf.keys = new ArrayList<K>(originKeys.subList(0, D));
		leaf.values = new ArrayList<T>(originValues.subList(0, D));
		// remove these copied keys and values from the original keys and values
		originKeys.subList(0, D).clear();
		originValues.subList(0, D).clear();
		// create a new right leaf node to store the rest of the original keys and values
		LeafNode<K,T> rightNode = new LeafNode<>(originKeys,originValues);
		// keep track on leaves
		rightNode.previousLeaf = leaf;
		rightNode.nextLeaf = leaf.nextLeaf;
		if(leaf.nextLeaf != null) leaf.nextLeaf.previousLeaf = rightNode;
		leaf.nextLeaf = rightNode;
		// obtain the key and value to return the entry of this new leaf node.
		K key = rightNode.keys.get(0);
		// if nodePath is empty, here means no indexNode or root.
		if(nodePath.empty()) {
			IndexNode<K, T> newRoot = new IndexNode<>(key, leaf, rightNode);
			root = newRoot;
		}
		return new SimpleEntry<>(key, rightNode);
	}

	/**
	 * TODO obtain the entry returned by leafNode split and handle the indexNode and return the new right node and the splitting
	 * key as an Entry<slitingKey, RightNode>
	 * 
	 * @param index
	 * @param entry
	 * @param nodePath
	 * @return new key/node pair as an Entry
	 */
	public void splitIndexNode(IndexNode<K,T> index, Entry<K, Node<K,T>> entry, Stack<IndexNode<K,T>> nodePath) {
		// find the correct position for entry.key in index node, using binary search
		int keyIndex = Collections.binarySearch(index.keys, entry.getKey());
		// indexNode.keys doesn't contain entry.key
		if(keyIndex < 0) {
			// get the correct position to insert
			keyIndex = -(keyIndex + 1);
		}
		// place the entry.key and entry.value to the correct position of index node .keys and .children
		index.insertSorted(entry, keyIndex);
		// check whether this index node is overflowed
		// if not overflowed, then return
		if(!index.isOverflowed()) return;
		// if overflowed
		ArrayList<K> originKeys = index.keys;
		ArrayList<Node<K, T>> originChildren = index.children;
		index.keys = new ArrayList<K>(originKeys.subList(0, D));
		index.children = new ArrayList<Node<K, T>>(originChildren.subList(0, D+1));
		originKeys.subList(0, D).clear();
		originChildren.subList(0, D+1).clear();
		// remove the key that is the entry
		K key = originKeys.remove(0);
		// create the right node
		IndexNode<K, T> rightNode = new IndexNode<>(originKeys, originChildren);
		// if nodePath is empty, which means no parent index node, create new index node and assign it to root.
		if(nodePath.empty()) {
			IndexNode<K, T> newRoot = new IndexNode<>(key, index, rightNode);
			root = newRoot;
		}
		// if not empty, means there exists parent index node, then merge the entry into the parent index node
		else {
			splitIndexNode(nodePath.pop(), new SimpleEntry<>(key, rightNode), nodePath);
		}
	}

	/**
	 * TODO Delete a key/value pair from this B+Tree
	 * 
	 * @param key
	 */
	public void delete(K key) {
		NodeInfo nodeInfo = new NodeInfo();
		searchLeafNode(key, root, nodeInfo);
		// if tree is empty, then return
		if(nodeInfo.leafNode == null) return;
		LeafNode<K,T> leafNode = nodeInfo.leafNode;
		Stack<IndexNode<K,T>> nodePath = nodeInfo.nodePath;
		Stack<Integer> indexPath = nodeInfo.indexPath;
		// find the correct position of the key in leafNode.keys
		int keyIndex = Collections.binarySearch(leafNode.keys, key);
		// if key not found, then return
		if(keyIndex < 0) return;
		// if keyIndex >= 0, then means position found
		// execute delete 
		leafNode.keys.remove(keyIndex);
		leafNode.values.remove(keyIndex);
		// handle leafNode underflowed
		if(leafNode.isUnderflowed()) {
			// check whether indexPath or nodePath is empty
			if(!indexPath.empty()) {
				// judge whether this leaf node has left sibling
				if(indexPath.peek() > 0) {
					// has left sibling
					handleLeafNodeUnderflow(leafNode.previousLeaf, leafNode, nodePath.peek(), indexPath.peek(), true);
				}
				else {
					// no left sibling
					handleLeafNodeUnderflow(leafNode, leafNode.nextLeaf, nodePath.peek(), indexPath.peek(), false);
				}
			}
			// handle indexNode
			while(!nodePath.empty()) {
				IndexNode<K,T> indexNode = nodePath.pop();
				indexPath.pop();
				// break if indexNode isn't underflowed or is root
				if(!indexNode.isUnderflowed() || indexNode.equals(root)) break;
				// judge whether this index node has left sibling
				if(!indexPath.empty()) {
					if(indexPath.peek() > 0) {
						// has left sibling
						handleIndexNodeUnderflow((IndexNode<K,T>)(nodePath.peek().children.get(indexPath.peek()-1)), 
								(IndexNode<K,T>)(nodePath.peek().children.get(indexPath.peek())), nodePath.peek(), indexPath.peek(), true);
					}
					else {
						// no left sibling
						handleIndexNodeUnderflow((IndexNode<K,T>)(nodePath.peek().children.get(indexPath.peek())), 
								(IndexNode<K,T>)(nodePath.peek().children.get(indexPath.peek()+1)), nodePath.peek(), indexPath.peek(), false);
					}
				}
			}
		}
	}

	/**
	 * TODO Handle LeafNode Underflow (merge or redistribution)
	 * 
	 * @param left
	 *            : the smaller node
	 * @param right
	 *            : the bigger node
	 * @param parent
	 *            : their parent index node
	 * @param parentIndex 
	 *            : this leafNode is at which index (insertion) of the parent node's keys
	 * @param hasLeftSibling
	 *            : whether or not has left sibling
	 */
	public void handleLeafNodeUnderflow(LeafNode<K,T> left, LeafNode<K,T> right, IndexNode<K,T> parent, int parentIndex, boolean hasLeftSibling) {
		// create keys and values
		ArrayList<K> keys = new ArrayList<>(left.keys);
		keys.addAll(new ArrayList<K>(right.keys));
		ArrayList<T> values = new ArrayList<>(left.values);
		values.addAll(new ArrayList<T>(right.values));
		// handle leafNode redistribution
		if(keys.size() >= 2*D) {
			int n = keys.size() / 2;
			List<K> removedKeys = keys.subList(0, n);
			List<T> removedValues = values.subList(0, n);
			// left leafNode
			left.keys = new ArrayList<K>(removedKeys);
			left.values = new ArrayList<T>(removedValues);
			removedKeys.clear();
			removedValues.clear();
			// right leafNode
			right.keys = keys;
			right.values = values;
			// handle its parent node's keys
			int keyIndex = hasLeftSibling ? parentIndex-1 : parentIndex;
			parent.keys.remove(keyIndex);
			parent.keys.add(keyIndex, right.keys.get(0));
		}
		// handle leafNode merge
		else {	
			// left is the target sibling
			if(hasLeftSibling) {
				left.keys = keys;
				left.values = values;
				// keep track on previous and next leafNode
				left.nextLeaf = right.nextLeaf;
				if(right.nextLeaf != null) right.nextLeaf.previousLeaf = left;
				// handle its parent node's keys and children's change
				parent.keys.remove(parentIndex-1);
				parent.children.remove(parentIndex);
				if(parent.equals(root) && parent.keys.size() == 0) {
					root = left;
				}
			}
			// right is the target sibling
			else {
				right.keys = keys;
				right.values = values;
				// keep track on previous and next leafNode
				right.previousLeaf = left.previousLeaf;
				if(left.previousLeaf != null) left.previousLeaf.nextLeaf = right;
				// handle its parent node's keys and children's change
				parent.keys.remove(parentIndex);
				parent.children.remove(parentIndex);
				if(parent.equals(root) && parent.keys.size() == 0) {
					root = right;
				}
			}
		}
	}

	/**
	 * TODO Handle IndexNode Underflow (merge or redistribution)
	 * 
	 * @param left
	 *            : the smaller node
	 * @param right
	 *            : the bigger node
	 * @param parent
	 *            : their parent index node
	 * @param parentIndex 
	 *            : this leafNode is at which index (insertion) of the parent node's keys
	 * @param hasLeftSibling
	 *            : whether or not has left sibling          
	 */
	public void handleIndexNodeUnderflow(IndexNode<K,T> leftIndex, IndexNode<K,T> rightIndex, IndexNode<K,T> parent, int parentIndex, boolean hasLeftSibling) {
		// put leftIndex and rightIndex 's keys and children together
		ArrayList<Node<K,T>> children = new ArrayList<>(leftIndex.children);
		children.addAll(new ArrayList<Node<K,T>>(rightIndex.children));
		// handle redistribution
		if((leftIndex.keys.size() + rightIndex.keys.size()) >= 2*D) {
			ArrayList<K> keys = new ArrayList<>(leftIndex.keys);
			keys.addAll(new ArrayList<K>(rightIndex.keys));
			int n = keys.size() / 2;
			List<K> removedKeys = keys.subList(0, n);
			List<Node<K,T>> removedChildren = children.subList(0, n+1);
			leftIndex.keys = new ArrayList<K>(removedKeys);
			leftIndex.children = new ArrayList<Node<K,T>>(removedChildren);
			removedKeys.clear();
			removedChildren.clear();
			rightIndex.keys = keys;
			rightIndex.children = children;
			int keyIndex = hasLeftSibling ? parentIndex-1 : parentIndex;
			K tmpKey = parent.keys.remove(keyIndex);
			if(hasLeftSibling) {
				parent.keys.add(keyIndex, rightIndex.keys.remove(0));
				rightIndex.keys.add(0, tmpKey);
			}
			else {
				parent.keys.add(keyIndex, leftIndex.keys.remove(leftIndex.keys.size()-1));
				leftIndex.keys.add(tmpKey);
			}

		}
		// handle merge
		else {
			
			// left is the target sibling
			if(hasLeftSibling) {
				ArrayList<K> keys = new ArrayList<>(leftIndex.keys);
				K keyTmp = parent.keys.remove(parentIndex-1);
				keys.add(keyTmp);
				keys.addAll(new ArrayList<K>(rightIndex.keys));
				parent.children.remove(parentIndex);
				leftIndex.children = children;
				leftIndex.keys = keys;
				if(parent.equals(root) && parent.keys.size() == 0) root = leftIndex;
			}
			// right is the target sibling
			else {
				ArrayList<K> keys = new ArrayList<>(leftIndex.keys);
				K keyTmp = parent.keys.remove(parentIndex);
				keys.add(keyTmp);
				keys.addAll(new ArrayList<K>(rightIndex.keys));
				parent.children.remove(parentIndex);
				rightIndex.children = children;
				rightIndex.keys = keys;
				if(parent.equals(root) && parent.keys.size() == 0) root = rightIndex;
			}
		}
	}
	
	/**
	 * inner class used to store node information:
	 * @field1 leafNode
	 * @field2 nodePath (stack to record indexNode along the path from root to leafNode)
	 * @field3 indexPath (stack to record the choice of index of the indexNode.keys along the path from root to leafNode)
	 * @author Xiaoyu Guo
	 */
	public class NodeInfo {
		private LeafNode<K,T> leafNode;
		private Stack<IndexNode<K,T>> nodePath;
		private Stack<Integer> indexPath;
		public NodeInfo() {
			nodePath = new Stack<IndexNode<K,T>>();
			indexPath = new Stack<Integer>();
		}
	}
}
