package ru.bmstu.iu9.db.zvoa.dbms.storage;

import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Key;

public class TestKey implements Key {
    public Integer id;

    public TestKey() {
    }

    public TestKey(int id) {
        this.id = id;
    }

    public Integer getId() {
        return id;
    }

    @Override
    public int compareTo(Object o) {
        return id - ((TestKey) o).id;
    }
}
