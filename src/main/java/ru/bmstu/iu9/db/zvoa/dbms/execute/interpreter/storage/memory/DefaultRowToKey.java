package ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory;

import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Key;

import java.io.Serializable;
import java.util.function.Function;

public class DefaultRowToKey implements Function<Row, Key>, Serializable {
    @Override
    public Key apply(Row row) {
        return new DefaultKey(row.hashCode());
    }
}