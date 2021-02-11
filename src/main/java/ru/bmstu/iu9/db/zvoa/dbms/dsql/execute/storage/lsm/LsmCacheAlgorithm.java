package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm;

import java.util.Map;

public interface LsmCacheAlgorithm<K extends Key, V extends Value> {
    public Map<K, V> swapMemory(Map<K, V> oldMap, Map<K, V> newData);
}
