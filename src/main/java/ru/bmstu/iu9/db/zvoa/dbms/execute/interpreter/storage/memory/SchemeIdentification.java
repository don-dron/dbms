package ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory;

import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Key;

public class SchemeIdentification implements Key {
    private String name;

    public SchemeIdentification(String name) {
        this.name = name;
    }

    @Override
    public int compareTo(Object o) {
        return name.compareTo(((SchemeIdentification) o).name);
    }
}
