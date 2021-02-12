package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.driver;

import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Key;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Value;

import java.io.Serializable;

public class Record<K extends Key, V extends Value> implements Serializable, Comparable {
    private K key;
    private V value;

    public Record() {
    }

    public Record(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public V getValue() {
        return value;
    }

    public K getKey() {
        return key;
    }

    @Override
    public int compareTo(Object object) {
        if (object instanceof Key) {
            return key.compareTo(((Key) object));
        } else if (object instanceof Record) {
            return key.compareTo(((Record) object).getKey());
        } else {
            throw new IllegalArgumentException("Not comparable key");
        }
    }
}
