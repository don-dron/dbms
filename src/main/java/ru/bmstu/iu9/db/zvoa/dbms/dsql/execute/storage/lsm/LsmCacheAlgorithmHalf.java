package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm;

import java.util.Map;
import java.util.TreeMap;

public class LsmCacheAlgorithmHalf<K extends Key, V extends Value> implements LsmCacheAlgorithm<K, V> {
    @Override
    public Map<K, V> swapMemory(Map<K, V> oldMap, Map<K, V> newData) {
        Map<K, V> map = new TreeMap<>();
        map.putAll(newData);
        return map;
    }
}
