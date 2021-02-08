package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.storage.memory;

import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.storage.driver.LSMStore;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.storage.driver.shared.KVItem;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.CreateTableSettings;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorageException;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Schema;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Table;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListSet;

public class DSQLSchema extends Schema {
    private final LSMStore lsmStore;
    private ConcurrentSkipListSet<Table> tables = new ConcurrentSkipListSet<>(Comparator.comparingInt(Table::hashCode));

    private DSQLSchema(Builder builder) throws DataStorageException, IOException {
        super(builder.schemaName);
        lsmStore = builder.lsmStore;
        initSchema();
    }

    private synchronized void initSchema() throws DataStorageException, IOException {
        for (String row : lsmStore.getAllKeys((row) -> true)) {
            tables.add(tableFromRow(row));
        }
    }

    private Table tableFromRow(String row) throws DataStorageException {
        try {
            String tableName = row;
            LSMStore tableStore = new LSMStore(Path.of(lsmStore.getRoot().toString() + "/" + tableName));
            DSQLTable table = DSQLTable.Builder.newBuilder()
                    .setName(tableName)
                    .setLsmStore(tableStore)
                    .build();
            return table;
        } catch (IOException exception) {
            throw new DataStorageException(exception.getMessage());
        }
    }

    @Override
    public synchronized Table createTable(CreateTableSettings settings) throws DataStorageException {
        try {
            if (tables.stream().anyMatch(schema -> schema.getTableName().equals(settings.getTableName()))) {
                throw new DataStorageException("Table already exist.");
            } else {
                LSMStore tableStore = new LSMStore(Path.of(lsmStore.getRoot().toString() + "/" + settings.getTableName()));
                DSQLTable table = DSQLTable.Builder.newBuilder()
                        .setName(settings.getTableName())
                        .setLsmStore(tableStore)
                        .build();
                lsmStore.put(new KVItem(settings.getTableName(), settings.getTableName()));
                tables.add(table);
                return table;
            }
        } catch (IOException ioException) {
            throw new DataStorageException("Cannot create files for table on driver: " + ioException.getMessage());
        }
    }

    @Override
    public synchronized Table getTable(String tableName) throws DataStorageException {
        return tables.stream()
                .filter(table -> table.getTableName().equals(tableName))
                .findFirst()
                .orElseThrow(() -> new DataStorageException("Table " + tableName + " not found"));
    }

    public static class Builder {
        private LSMStore lsmStore;
        private String schemaName;

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder setLsmStore(LSMStore lsmStore) {
            this.lsmStore = lsmStore;
            return this;
        }

        public Builder setSchemaName(String schemaName) {
            this.schemaName = schemaName;
            return this;
        }

        public DSQLSchema build() throws DataStorageException, IOException {
            return new DSQLSchema(this);
        }
    }
}
