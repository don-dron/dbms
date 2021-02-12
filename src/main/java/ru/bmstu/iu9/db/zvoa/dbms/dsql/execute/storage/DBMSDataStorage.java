package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.FileSystemManager;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.memory.DSQLSchema;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.memory.DSQLTable;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.*;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Row;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Schema;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Table;
import ru.bmstu.iu9.db.zvoa.dbms.modules.AbstractDbModule;
import ru.bmstu.iu9.db.zvoa.dbms.modules.IDbModule;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;

public class DBMSDataStorage extends AbstractDbModule implements DataStorage {
    private final Logger logger = LoggerFactory.getLogger(DBMSDataStorage.class);
    private final FileSystemManager fileSystemManager;
    private IKeyValueStorage<Schema.SchemeIdentification, DSQLSchema> fileSchemasStorage;
    private ConcurrentSkipListSet<Schema> schemas = new ConcurrentSkipListSet<>(Comparator.comparingInt(Schema::hashCode));

    private DBMSDataStorage(Builder builder) {
        assert (builder.fileSystemManager != null);
        this.fileSystemManager = builder.fileSystemManager;
    }

    @Override
    public synchronized void init() throws DataStorageException {
        if (isInit()) {
            return;
        }

        ExecutorService executorService = createExecutorService();

        initModules(executorService);

        setRunning();
        logRunning();
        executorService.shutdown();
        joinExecutorService(executorService);

        linkStorages();

        logInit();
        setInit();
    }

    @Override
    public void run() {
        ExecutorService executorService;
        synchronized (this) {
            if (isRunning()) {
                return;
            }
            executorService = createExecutorService();

            runModules(executorService);
            setRunning();
            logRunning();
        }

        executorService.shutdown();
        joinExecutorService(executorService);
    }

    @Override
    public void close() throws Exception {
        synchronized (this) {
            if (isClosed()) {
                return;
            }
            fileSystemManager.close();
            setClosed();
            logClose();
        }
    }

    private void joinExecutorService(ExecutorService executorService) {
        while (true) {
            try {
                if (executorService.awaitTermination(1, TimeUnit.SECONDS)) {
                    break;
                } else {
                    Thread.onSpinWait();
                }
            } catch (InterruptedException exception) {
                logger.warn(exception.getMessage());
            }
        }
    }

    private ExecutorService createExecutorService() {
        return new ThreadPoolExecutor(
                16,
                Integer.MAX_VALUE,
                Integer.MAX_VALUE,
                TimeUnit.DAYS,
                new SynchronousQueue<>());
    }

    private void initModules(ExecutorService executorService) {
        initModule(fileSystemManager, executorService);
    }

    private void runModules(ExecutorService executorService) {
        runModule(fileSystemManager, executorService);
    }

    private void initModule(IDbModule module, ExecutorService executorService) {
        executorService.execute(
                () -> {
                    try {
                        module.init();
                    } catch (Exception exception) {
                        logger.error(exception.getMessage());
                    }
                });
    }

    private void linkStorages() throws DataStorageException {
        fileSchemasStorage = fileSystemManager.getRootStorage();

        fileSystemManager
                .getCurrentSchemas()
                .entrySet()
                .stream()
                .forEach(entry -> {
                    String name = entry.getKey();
                    IKeyValueStorage storage = entry.getValue();
                    DSQLSchema schema = DSQLSchema.Builder.newBuilder()
                            .setSchemaName(name)
                            .setStorage(storage)
                            .build();
                    schemas.add(schema);

                    fileSystemManager.getCurrentTables(name).entrySet()
                            .stream()
                            .forEach(tableEntry -> {
                                String tableName = entry.getKey();
                                IKeyValueStorage tableStorage = entry.getValue();
                                DSQLTable table = DSQLTable.Builder.newBuilder()
                                        .setName(tableName)
                                        .setStorage(tableStorage)
                                        .build();
                                try {
                                    schema.addTable(table);
                                } catch (DataStorageException dataStorageException) {
                                    dataStorageException.printStackTrace();
                                }
                            });
                });
    }

    private void runModule(IDbModule dbModule, ExecutorService executorService) {
        executorService.execute(dbModule);
    }

    public Table createTable(CreateTableSettings settings) throws DataStorageException, IOException {
        Schema schema = useSchema(settings.getSchemaName());

        if (schema.getTable(settings.getTableName()) != null) {
            throw new DataStorageException("Table already exist.");
        } else {
            IKeyValueStorage tableStorage = fileSystemManager.createTableStorage(settings);
            DSQLTable table = DSQLTable.Builder.newBuilder()
                    .setName(settings.getTableName())
                    .setStorage(tableStorage)
                    .build();
            tableStorage.init();
            schema.addTable(table);
            return table;
        }
    }

    public Schema createSchema(CreateSchemaSettings settings) throws DataStorageException, IOException {
        if (schemas.stream().anyMatch(schema -> schema.getSchemaName().equals(settings.getSchemaName()))) {
            throw new DataStorageException("Schema already exist.");
        } else {
            IKeyValueStorage schemaStorage = fileSystemManager.createSchemaStorage(settings);
            DSQLSchema schema = DSQLSchema.Builder.newBuilder()
                    .setSchemaName(settings.getSchemaName())
                    .setStorage(schemaStorage)
                    .build();
            schemaStorage.init();
            fileSchemasStorage.put(new Schema.SchemeIdentification(schema.getSchemaName()), schema);
            schemas.add(schema);
            return schema;
        }
    }

    @Override
    public List<Row> insertRows(InsertSettings insertSettings) throws DataStorageException {
        return useSchema(insertSettings.getSchemaName())
                .useTable(insertSettings.getTableName())
                .insertRows(insertSettings);
    }

    @Override
    public List<Row> selectRows(SelectSettings selectSettings) throws DataStorageException {
        return useSchema(selectSettings.getSchemaName())
                .useTable(selectSettings.getTableName())
                .selectRows(selectSettings);
    }

    @Override
    public List<Row> deleteRows(DeleteSettings deleteSettings) throws DataStorageException {
        return useSchema(deleteSettings.getSchemaName())
                .useTable(deleteSettings.getTableName())
                .deleteRows(deleteSettings);
    }

    public Schema getSchema(String schemaName) throws DataStorageException {
        Schema schema = schemas.stream()
                .filter(sch -> sch.getSchemaName().equals(schemaName))
                .findFirst()
                .orElse(null);
        return schema;
    }

    private Schema useSchema(String schemaName) throws DataStorageException {
        Schema schema = schemas.stream()
                .filter(sch -> sch.getSchemaName().equals(schemaName))
                .findFirst()
                .orElseThrow(() -> new DataStorageException("Undefined schema " + schemaName));
        return schema;
    }

    public static class Builder {
        private FileSystemManager fileSystemManager;

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder setFileSystemManager(FileSystemManager fileSystemManager) {
            this.fileSystemManager = fileSystemManager;
            return this;
        }

        public DBMSDataStorage build() throws IOException, DataStorageException {
            return new DBMSDataStorage(this);
        }
    }
}
