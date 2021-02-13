package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm;

import java.util.List;

public interface MemoryMap<K, V> {
    public V search(K key);

    public List<V> searchRange(K key1, K key2);

    public V insert(K key, V value);

    public V delete(K key);
}
