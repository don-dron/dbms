package ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory;

import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Key;

public class DefaultKey implements Key {
    private long id;

    public DefaultKey(long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }

    @Override
    public int compareTo(Object o) {
        return Long.compare(id, ((DefaultKey) o).id);
    }
}
