package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.LSMStore;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.shared.KVItem;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.IKeyValueStorage;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.memory.DSQLSchema;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.*;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Schema;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Table;
import ru.bmstu.iu9.db.zvoa.dbms.modules.AbstractDbModule;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;

public class DBMSDataStorage extends AbstractDbModule implements DataStorage {
    private final Logger logger = LoggerFactory.getLogger(DBMSDataStorage.class);
    private final FileSystemMount lsmStore;
    private ConcurrentSkipListSet<Schema> schemas = new ConcurrentSkipListSet<>(Comparator.comparingInt(Schema::hashCode));

    private DBMSDataStorage(Builder builder) throws IOException, DataStorageException {
        assert (builder.lsmStore != null);
        this.lsmStore = builder.lsmStore;
        initStorage();
    }

    @Override
    public void init() {
        synchronized (this) {
            if (isInit()) {
                return;
            }
            logInit();
            setInit();
        }
    }

    @Override
    public void run() {
        synchronized (this) {
            if (isRunning()) {
                return;
            }
            setRunning();
            logRunning();
        }

        lsmStore.start();
        try {
            lsmStore.join();
        } catch (InterruptedException exception) {
            throw new IllegalStateException(exception.getMessage());
        }
    }

    @Override
    public void close() throws Exception {
        synchronized (this) {
            if (isClosed()) {
                return;
            }
            lsmStore.shutdown();

            setClosed();
            logClose();
        }
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
                .insertRows(insertSettings);
    }

    @Override
    public synchronized List<Table.Row> selectRows(SelectSettings selectSettings) throws DataStorageException {
        return getSchema(selectSettings.getSchemaName())
                .getTable(selectSettings.getTableName())
                .selectRows(selectSettings);
    }

    @Override
    public synchronized List<Table.Row> deleteRows(DeleteSettings deleteSettings) throws DataStorageException {
        return getSchema(deleteSettings.getSchemaName())
                .getTable(deleteSettings.getTableName())
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
        private IKeyValueStorage lsmStore;

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder setLsmStore(IKeyValueStorage lsmStore) {
            this.lsmStore = lsmStore;
            return this;
        }

        public DBMSDataStorage build() throws IOException, DataStorageException {
            return new DBMSDataStorage(this);
        }
    }
}
