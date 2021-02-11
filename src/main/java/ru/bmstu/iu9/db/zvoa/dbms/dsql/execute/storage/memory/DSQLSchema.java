package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.memory;

import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.IKeyValueStorage;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Value;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorageException;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Schema;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Table;

import java.util.Comparator;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

public class DSQLSchema extends Schema {
    private final IKeyValueStorage storage;
    private final ConcurrentSkipListSet<Table> tables;

    private DSQLSchema(Builder builder) {
        super(builder.schemaName);
        storage = builder.storage;
        tables = builder.tables;
    }

    @Override
    public boolean addTable(Table table) {
        return tables.add(table);
    }

    @Override
    public synchronized Table getTable(String tableName) throws DataStorageException {
        return tables.stream()
                .filter(table -> table.getTableName().equals(tableName))
                .findFirst()
                .orElseThrow(() -> new DataStorageException("Table " + tableName + " not found"));
    }

    public class DSQLSchemaValue implements Value {
        private String schemaName;
        private String path;

        public DSQLSchemaValue(String schemaName, String path) {
            this.schemaName = schemaName;
            this.path = path;
        }

        public String getPath() {
            return path;
        }

        public String getSchemaName() {
            return schemaName;
        }
    }

    public static class Builder {
        private IKeyValueStorage storage;
        private String schemaName;
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

        public Builder setTables(Set<Table> tables) {
            this.tables = new ConcurrentSkipListSet<>(tables);
            return this;
        }

        public DSQLSchema build() {
            return new DSQLSchema(this);
        }
    }
}
