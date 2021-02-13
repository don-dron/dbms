package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm;

import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class LsmCacheAlgorithmHalf<K extends Key, V extends Value> implements LsmCacheAlgorithm<K, V> {

    @Override
    public Map<K, V> swapMemory(Map<K, V> oldMap, Map<K, V> newData) {
        Map<K, V> result =
                newData.entrySet()
                        .stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue,
                                (v1, v2) -> v1,
                                () -> new TreeMap<>(oldMap)));
        return result;
    }
}
