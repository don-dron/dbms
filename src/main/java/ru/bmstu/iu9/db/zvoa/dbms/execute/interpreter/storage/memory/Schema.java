package ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory;

import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.CreateTableSettings;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorageException;

import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListSet;

public abstract class Schema {
    private ConcurrentSkipListSet<Table> tables = new ConcurrentSkipListSet<>(Comparator.comparingInt(Table::hashCode));

    private final String schemaName;

    public Schema(String schemaName) {
        this.schemaName = schemaName;
    }

    public abstract Table createTable(CreateTableSettings settings) throws DataStorageException;

    public abstract Table getTable(String tableName) throws DataStorageException;

    public String getSchemaName() {
        return schemaName;
    }
}
