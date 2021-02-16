package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.driver;

import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Key;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Value;

import java.io.Serializable;

public class Meta<K extends Key, V extends Value> implements Serializable {
    private K[] keys;

    public Meta() {
    }

    public Meta(K[] keys) {
        this.keys = keys;
    }

    public K[] getKeys() {
        return keys;
    }
}
