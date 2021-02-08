package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.storage.driver.LSMStore;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.storage.driver.shared.KVItem;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.storage.memory.DSQLSchema;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.*;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Schema;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Table;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;

public class DBMSDataStorage implements DataStorage {
    private final Logger logger = LoggerFactory.getLogger(DBMSDataStorage.class);
    private final LSMStore lsmStore;
    private ConcurrentSkipListSet<Schema> schemas = new ConcurrentSkipListSet<>(Comparator.comparingInt(Schema::hashCode));

    private DBMSDataStorage(Builder builder) throws IOException, DataStorageException {
        assert (builder.lsmStore != null);
        this.lsmStore = builder.lsmStore;

        initStorage();
    }

    private synchronized void initStorage() throws DataStorageException, IOException {
        for (String row : lsmStore.getAllKeys((row) -> true)) {
            schemas.add(schemaFromRow(row));
        }
    }

    private Schema schemaFromRow(String row) throws DataStorageException {
        try {
            String schemaName = row;
            LSMStore schemaStore = new LSMStore(Path.of(lsmStore.getRoot().toString() + "/" + schemaName));
            DSQLSchema schema = DSQLSchema.Builder.newBuilder()
                    .setSchemaName(schemaName)
                    .setLsmStore(schemaStore)
                    .build();
            return schema;
        } catch (IOException exception) {
            throw new DataStorageException(exception.getMessage());
        }
    }

    @Override
    public synchronized Schema createSchema(CreateSchemaSettings settings) throws DataStorageException {
        try {
            if (schemas.stream().anyMatch(schema -> schema.getSchemaName().equals(settings.getSchemaName()))) {
                throw new DataStorageException("Schema already exist.");
            } else {
                LSMStore schemaStore = new LSMStore(Path.of(lsmStore.getRoot().toString() + "/" + settings.getSchemaName()));
                DSQLSchema schema = DSQLSchema.Builder.newBuilder()
                        .setSchemaName(settings.getSchemaName())
                        .setLsmStore(schemaStore)
                        .build();

                lsmStore.put(new KVItem(settings.getSchemaName(), settings.getSchemaName()));
                schemas.add(schema);
                return schema;
            }
        } catch (IOException ioException) {
            throw new DataStorageException("Cannot create files for schema on driver: " + ioException.getMessage());
        }
    }

    @Override
    public synchronized Table createTable(CreateTableSettings settings) throws DataStorageException {
        return getSchema(settings.getSchemaName()).createTable(settings);
    }

    @Override
    public synchronized List<Table.Row> insertRows(InsertSettings insertSettings) throws DataStorageException {
        return getSchema(insertSettings.getSchemaName())
                .getTable(insertSettings.getTableName())
                .orElseThrow(() -> new DataStorageException("Table " + insertSettings.getTableName() + " not found"))
                .insertRows(insertSettings);
    }

    @Override
    public synchronized List<Table.Row> selectRows(SelectSettings selectSettings) throws DataStorageException {
        return getSchema(selectSettings.getSchemaName())
                .getTable(selectSettings.getTableName())
                .orElseThrow(() -> new DataStorageException("Table " + selectSettings.getTableName() + " not found"))
                .selectRows(selectSettings);
    }

    @Override
    public synchronized List<Table.Row> deleteRows(DeleteSettings deleteSettings) throws DataStorageException {
        return getSchema(deleteSettings.getSchemaName())
                .getTable(deleteSettings.getTableName())
                .orElseThrow(() -> new DataStorageException("Table " + deleteSettings.getTableName() + " not found"))
                .deleteRows(deleteSettings);
    }

    private Schema getSchema(String schemaName) throws DataStorageException {
        Schema schema = schemas.stream()
                .filter(sch -> sch.getSchemaName().equals(schemaName))
                .findFirst()
                .orElseThrow(() -> new DataStorageException("Undefined schema " + schemaName));
        return schema;
    }

    public static class Builder {
        private LSMStore lsmStore;

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder setLsmStore(LSMStore lsmStore) {
            this.lsmStore = lsmStore;
            return this;
        }

        public DBMSDataStorage build() throws IOException, DataStorageException {
            return new DBMSDataStorage(this);
        }
    }
}
