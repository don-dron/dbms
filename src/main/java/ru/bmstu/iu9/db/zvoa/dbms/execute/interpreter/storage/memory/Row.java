package ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory;


import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Key;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Value;

import java.util.List;

public class Row implements Value {
    private transient Table table;
    private List<Object> values;

    public Row(Table table, List<Object> values) {
        this.table = table;
        this.values = values;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public Key getKey() {
        return table.getRowKey(this);
    }

    public Table getTable() {
        return table;
    }

    public List<Object> getValues() {
        return values;
    }

    @Override
    public String toString() {
        return "Row{" +
                "values=" + values +
                '}';
    }
}