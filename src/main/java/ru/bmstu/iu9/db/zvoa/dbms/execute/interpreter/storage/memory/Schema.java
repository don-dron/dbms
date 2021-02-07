package ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory;

import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.storage.memory.DSQLTable;
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

    public synchronized Table createTable(CreateTableSettings settings) throws DataStorageException {
        Table table = DSQLTable.Builder.newBuilder()
                .setName(settings.getTableName())
                .setTypes(settings.getTypes()).build();

        if (tables.add(table)) {
            return table;
        } else {
            throw new DataStorageException("Table " + table + " already exist");
        }
    }

    public synchronized Optional<Table> getTable(String tableName) {
        return tables.stream()
                .filter(table -> table.getTableName().equals(tableName))
                .findFirst();
    }

    public String getSchemaName() {
        return schemaName;
    }
}
