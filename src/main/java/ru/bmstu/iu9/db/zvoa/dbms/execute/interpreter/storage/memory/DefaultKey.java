package ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory;

import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Key;

public class DefaultKey implements Key {
    private int id;

    public DefaultKey(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    @Override
    public int compareTo(Object o) {
        return id - ((DefaultKey) o).id;
    }
}
