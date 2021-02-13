package ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory;

import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Key;

import java.io.Serializable;

public class TableIdentification implements Key {
    private String name;

    public TableIdentification(String name) {
        this.name = name;
    }

    @Override
    public int compareTo(Object o) {
        return name.compareTo(((TableIdentification) o).name);
    }
}
