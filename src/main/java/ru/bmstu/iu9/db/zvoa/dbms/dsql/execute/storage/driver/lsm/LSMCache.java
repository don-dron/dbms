package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.lsm;

import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.shared.Constants;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.shared.KVItem;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LSMCache {
    TreeMap<String, KVItem> lsmCache = new TreeMap<>();

    final ReadWriteLock rwl = new ReentrantReadWriteLock();

    public void put(KVItem item) {
        try {
            rwl.writeLock().lock();
            lsmCache.put(item.getKey(), item);
        } finally {
            rwl.writeLock().unlock();
        }
    }

    public KVItem get(String key) {
        try{
            rwl.readLock().lock();
            return lsmCache.get(key);
        } finally {
            rwl.readLock().unlock();
        }
    }

    public TreeMap<String, KVItem> getSnapshot() {
        rwl.writeLock().lock();

        try {
            TreeMap<String, KVItem> snapshot = lsmCache;
            lsmCache = new TreeMap<>();
            return snapshot;
        } finally {
            rwl.writeLock().unlock();
        }
    }

    public TreeMap<String, KVItem> getShallowLsmCopy() {
        return new TreeMap<>(lsmCache);
    }

    public int size() {
        return lsmCache.size();
    }

    public Set<KVItem> scan (String key){
        Set<KVItem> matchingList = new HashSet<>();
        try {
            rwl.readLock().lock();
            for (String k : this.lsmCache.keySet()){
                if (k.contains(key) && !this.lsmCache.get(k).getValue().equals(Constants.DELETE_MARKER)){
                    matchingList.add(this.lsmCache.get(k));
                }
            }
            return matchingList;
        } finally {
            rwl.readLock().unlock();
        }
    }
}
