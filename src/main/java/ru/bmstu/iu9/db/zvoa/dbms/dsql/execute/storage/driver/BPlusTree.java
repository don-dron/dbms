/*
 * Copyright (c) 2021 Zvorygin Andrey BMSTU IU-9 https://github.com/don-dron
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
 * OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT
 * OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * B+ tree implementation. Not thread safe!
 *
 * @param <K> - type of keys
 * @param <V> - type of values
 */
public class BPlusTree<K extends Comparable<? super K>, V> implements Map<K, V> {
    private static final int DEFAULT_BRANCHING_FACTOR = 128;
    private final int branchingFactor;
    private Node root;

    /**
     * Create B+ tree with default branching factor.
     */
    public BPlusTree() {
        this(DEFAULT_BRANCHING_FACTOR);
    }

    /**
     * Create B+ tree with custom branching factor.
     *
     * @param branchingFactor - factor
     */
    public BPlusTree(int branchingFactor) {
        if (branchingFactor <= 2)
            throw new IllegalArgumentException("Illegal branching factor: "
                    + branchingFactor);
        this.branchingFactor = branchingFactor;
        root = new LeafNode();
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean containsKey(Object key) {
        return false;
    }

    @Override
    public boolean containsValue(Object value) {
        return false;
    }

    @Override
    public void clear() {
    }

    @Override
    public @NotNull Set<K> keySet() {
        return null;
    }

    @Override
    public @NotNull Collection<V> values() {
        return null;
    }

    @Override
    public @NotNull Set<Entry<K, V>> entrySet() {
        return null;
    }

    public V get(Object key) {
        return root.getValue((K) key);
    }

    public List<V> searchRange(K key1, K key2) {
        return root.getRange(key1, key2);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
    }

    public V put(K key, V value) {
        return root.insertValue(key, value);
    }

    public V remove(Object key) {
        return root.deleteValue((K) key);
    }

    @Override
    public String toString() {
        Queue<List<Node>> queue = new LinkedList<>();
        queue.add(Arrays.asList(root));
        StringBuilder sb = new StringBuilder();
        sb.append("Branching factor: " + branchingFactor + System.lineSeparator());
        while (!queue.isEmpty()) {
            Queue<List<Node>> nextQueue = new LinkedList<>();
            while (!queue.isEmpty()) {
                List<Node> nodes = queue.remove();
                sb.append("{");
                Iterator<Node> it = nodes.iterator();
                while (it.hasNext()) {
                    Node node = it.next();
                    sb.append(node.toString());
                    if (it.hasNext())
                        sb.append(", ");
                    if (node instanceof BPlusTree.InternalNode)
                        nextQueue.add(((InternalNode) node).children);
                }
                sb.append("}");
                if (!queue.isEmpty())
                    sb.append(", ");
                else
                    sb.append(System.lineSeparator());
            }
            queue = nextQueue;
        }

        return sb.toString();
    }

    private abstract class Node {
        List<K> keys;

        int keyNumber() {
            return keys.size();
        }

        abstract V getValue(K key);

        abstract V deleteValue(K key);

        abstract V insertValue(K key, V value);

        abstract K getFirstLeafKey();

        abstract List<V> getRange(K key1, K key2);

        abstract void merge(Node sibling);

        abstract Node split();

        abstract boolean isOverflow();

        abstract boolean isUnderflow();

        public String toString() {
            return keys.toString();
        }
    }

    private class InternalNode extends Node {
        List<Node> children;

        InternalNode() {
            this.keys = new ArrayList<>();
            this.children = new ArrayList<>();
        }

        @Override
        V getValue(K key) {
            return getChild(key).getValue(key);
        }

        @Override
        V deleteValue(K key) {
            Node child = getChild(key);
            V v = child.deleteValue(key);
            if (child.isUnderflow()) {
                Node childLeftSibling = getChildLeftSibling(key);
                Node childRightSibling = getChildRightSibling(key);
                Node left = childLeftSibling != null ? childLeftSibling : child;
                Node right = childLeftSibling != null ? child
                        : childRightSibling;
                left.merge(right);

                assert right != null;
                deleteChild(right.getFirstLeafKey());

                if (left.isOverflow()) {
                    Node sibling = left.split();
                    insertChild(sibling.getFirstLeafKey(), sibling);
                }
                if (root.keyNumber() == 0)
                    root = left;
            }
            return v;
        }

        @Override
        V insertValue(K key, V value) {
            Node child = getChild(key);
            V v = child.insertValue(key, value);
            if (child.isOverflow()) {
                Node sibling = child.split();
                insertChild(sibling.getFirstLeafKey(), sibling);
            }
            if (root.isOverflow()) {
                Node sibling = split();
                InternalNode newRoot = new InternalNode();
                newRoot.keys.add(sibling.getFirstLeafKey());
                newRoot.children.add(this);
                newRoot.children.add(sibling);
                root = newRoot;
            }
            return v;
        }

        @Override
        K getFirstLeafKey() {
            return children.get(0).getFirstLeafKey();
        }

        @Override
        List<V> getRange(K key1, K key2) {
            return getChild(key1).getRange(key1, key2);
        }

        @Override
        void merge(Node sibling) {
            InternalNode node = (InternalNode) sibling;
            keys.add(node.getFirstLeafKey());
            keys.addAll(node.keys);
            children.addAll(node.children);

        }

        @Override
        Node split() {
            int from = keyNumber() / 2 + 1, to = keyNumber();
            InternalNode sibling = new InternalNode();
            sibling.keys.addAll(keys.subList(from, to));
            sibling.children.addAll(children.subList(from, to + 1));

            keys.subList(from - 1, to).clear();
            children.subList(from, to + 1).clear();

            return sibling;
        }

        @Override
        boolean isOverflow() {
            return children.size() > branchingFactor;
        }

        @Override
        boolean isUnderflow() {
            return children.size() < (branchingFactor + 1) / 2;
        }

        Node getChild(K key) {
            int loc = Collections.binarySearch(keys, key);
            int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
            return children.get(childIndex);
        }

        void deleteChild(K key) {
            int loc = Collections.binarySearch(keys, key);
            if (loc >= 0) {
                keys.remove(loc);
                children.remove(loc + 1);
            }
        }

        void insertChild(K key, Node child) {
            int loc = Collections.binarySearch(keys, key);
            int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
            if (loc >= 0) {
                children.set(childIndex, child);
            } else {
                keys.add(childIndex, key);
                children.add(childIndex + 1, child);
            }
        }

        @Nullable Node getChildLeftSibling(K key) {
            int loc = Collections.binarySearch(keys, key);
            int childIndex = loc >= 0 ? loc + 1 : -loc - 1;

            if (childIndex > 0) {
                return children.get(childIndex - 1);
            }

            return null;
        }

        @Nullable Node getChildRightSibling(K key) {
            int loc = Collections.binarySearch(keys, key);
            int childIndex = loc >= 0 ? loc + 1 : -loc - 1;

            if (childIndex < keyNumber()) {
                return children.get(childIndex + 1);
            }

            return null;
        }

        @Override
        public String toString() {
            return "InternalNode{" +
                    "children=" + children +
                    "}";
        }
    }

    private class LeafNode extends Node {
        List<V> values;
        @Nullable LeafNode next;

        LeafNode() {
            keys = new ArrayList<>();
            values = new ArrayList<>();
            next = null;
        }

        @Override
        @Nullable
        V getValue(K key) {
            int loc = Collections.binarySearch(keys, key);
            return loc >= 0 ? values.get(loc) : null;
        }

        @Override
        @Nullable
        V deleteValue(K key) {
            int loc = Collections.binarySearch(keys, key);
            if (loc >= 0) {
                keys.remove(loc);
                return values.remove(loc);
            }
            return null;
        }

        @Override
        V insertValue(K key, V value) {
            int loc = Collections.binarySearch(keys, key);
            int valueIndex = loc >= 0 ? loc : -loc - 1;
            V v = null;
            if (loc >= 0) {
                v = values.set(valueIndex, value);
            } else {
                keys.add(valueIndex, key);
                values.add(valueIndex, value);
            }
            if (root.isOverflow()) {
                Node sibling = split();
                InternalNode newRoot = new InternalNode();
                newRoot.keys.add(sibling.getFirstLeafKey());
                newRoot.children.add(this);
                newRoot.children.add(sibling);
                root = newRoot;
            }
            return v;
        }

        @Override
        K getFirstLeafKey() {
            return keys.get(0);
        }

        @Override
        List<V> getRange(K key1, K key2) {
            List<V> result = new LinkedList<>();
            LeafNode node = this;

            while (node != null) {
                Iterator<K> kIt = node.keys.iterator();
                Iterator<V> vIt = node.values.iterator();
                while (kIt.hasNext()) {
                    K key = kIt.next();
                    V value = vIt.next();
                    int cmp1 = key.compareTo(key1);
                    int cmp2 = key.compareTo(key2);
                    if (cmp1 > 0 && cmp2 < 0) {
                        result.add(value);
                    } else if (cmp2 >= 0) {
                        return result;
                    }
                }
                node = node.next;
            }
            return result;
        }

        @Override
        void merge(Node sibling) {
            LeafNode node = (LeafNode) sibling;
            keys.addAll(node.keys);
            values.addAll(node.values);
            next = node.next;
        }

        @Override
        Node split() {
            LeafNode sibling = new LeafNode();
            int from = (keyNumber() + 1) / 2, to = keyNumber();
            sibling.keys.addAll(keys.subList(from, to));
            sibling.values.addAll(values.subList(from, to));

            keys.subList(from, to).clear();
            values.subList(from, to).clear();

            sibling.next = next;
            next = sibling;
            return sibling;
        }

        @Override
        boolean isOverflow() {
            return values.size() > branchingFactor - 1;
        }

        @Override
        boolean isUnderflow() {
            return values.size() < branchingFactor / 2;
        }

        @Override
        public String toString() {
            return "LeafNode{" +
                    "values=" + values +
                    ", next=" + next +
                    "}";
        }
    }
}