package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.memory;

import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.IKeyValueStorage;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorageException;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Schema;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Table;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.TableIdentification;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

public class DSQLSchema extends Schema {
    private transient final IKeyValueStorage<TableIdentification, Table> storage;
    private transient final ConcurrentSkipListSet<Table> tables;

    private DSQLSchema(Builder builder) {
        super(builder.schemaName, builder.schemaPath);
        storage = builder.storage;
        tables = builder.tables;
    }

    @Override
    public List<Table> getTables() {
        return Arrays.asList(tables.toArray(Table[]::new));
    }

    @Override
    public boolean addTable(Table table) throws DataStorageException {
        if (tables.add(table)) {
            storage.put(new TableIdentification(table.getTableName()), table);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean deleteTable(Table table) throws DataStorageException {
        if (tables.remove(table)) {
            storage.put(new TableIdentification(table.getTableName()), null);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public synchronized Table getTable(String tableName) throws DataStorageException {
        return tables.stream()
                .filter(table -> table.getTableName().equals(tableName))
                .findFirst()
                .orElse(null);
    }

    @Override
    public synchronized Table useTable(String tableName) throws DataStorageException {
        return tables.stream()
                .filter(table -> table.getTableName().equals(tableName))
                .findFirst()
                .orElseThrow(() -> new DataStorageException("Table " + tableName + " not found"));
    }

    public static class Builder {
        private IKeyValueStorage storage;
        private String schemaName;
        private String schemaPath;
        private ConcurrentSkipListSet<Table> tables = new ConcurrentSkipListSet<>(Comparator.comparingInt(Table::hashCode));

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder setStorage(IKeyValueStorage storage) {
            this.storage = storage;
            return this;
        }

        public Builder setSchemaName(String schemaName) {
            this.schemaName = schemaName;
            return this;
        }

        public void setSchemaPath(String schemaPath) {
            this.schemaPath = schemaPath;
        }

        public Builder setTables(Set<Table> tables) {
            this.tables = new ConcurrentSkipListSet<>(tables);
            return this;
        }

        public DSQLSchema build() {
            return new DSQLSchema(this);
        }
    }
}
