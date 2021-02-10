package ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory;

import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorageException;

public abstract class Schema {
    private final String schemaName;

    public Schema(String schemaName) {
        this.schemaName = schemaName;
    }

    public abstract boolean addTable(Table table);

    public abstract Table getTable(String tableName) throws DataStorageException;

    public String getSchemaName() {
        return schemaName;
    }
}
