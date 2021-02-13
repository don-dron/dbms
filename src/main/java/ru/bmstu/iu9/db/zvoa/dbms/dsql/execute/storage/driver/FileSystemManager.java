package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.IKeyValueStorage;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.memory.DSQLSchema;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.memory.DSQLTable;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.CreateSchemaSettings;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.CreateTableSettings;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorageException;
import ru.bmstu.iu9.db.zvoa.dbms.modules.AbstractDbModule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class FileSystemManager extends AbstractDbModule {
    private final ExecutorService executorService;
    private final Logger logger = LoggerFactory.getLogger(FileSystemManager.class);
    private final ConcurrentHashMap<FileItem, IKeyValueStorage> itemToStorage = new ConcurrentHashMap<>();
    private final StorageProperties rootDirectory;
    private final Function<StorageProperties, IKeyValueStorage> storageSupplier;

    public FileSystemManager(FileSystemManagerConfig fileSystemManagerConfig) {
        rootDirectory = fileSystemManagerConfig.getStorageProperties();
        storageSupplier = fileSystemManagerConfig.getStorageSupplier();
        executorService = createExecutorService();
    }

    public synchronized IKeyValueStorage createTableStorage(CreateTableSettings settings) throws DataStorageException, IOException {
        StorageProperties storageProperties = new StorageProperties(settings.getTableName(),
                rootDirectory.getPath() + "/" + settings.getSchemaName() + "/" + settings.getTableName());
        IKeyValueStorage storage = createStorage(storageProperties);
        FileItem fileItem = new FileItem(settings.getSchemaName(), settings.getTableName(), StorageProperties.StorageType.TABLE);
        itemToStorage.put(fileItem, storage);
        return storage;
    }

    public synchronized IKeyValueStorage createSchemaStorage(CreateSchemaSettings settings) throws DataStorageException, IOException {
        StorageProperties storageProperties = new StorageProperties(settings.getSchemaName(),
                rootDirectory.getPath() + "/" + settings.getSchemaName());
        IKeyValueStorage storage = createStorage(storageProperties);
        FileItem fileItem = new FileItem(settings.getSchemaName(), null, StorageProperties.StorageType.SCHEMA);
        itemToStorage.put(fileItem, storage);
        return storage;
    }

    public synchronized IKeyValueStorage getRootStorage() throws DataStorageException {
        return itemToStorage.entrySet().stream()
                .filter(entry -> entry.getKey().getType() == StorageProperties.StorageType.ROOT)
                .findFirst()
                .orElseThrow(() -> new DataStorageException("Not found root storage."))
                .getValue();
    }

    public synchronized ConcurrentMap<String, IKeyValueStorage> getCurrentSchemas() {
        return itemToStorage.entrySet().stream()
                .filter(entry -> entry.getKey().getType() == StorageProperties.StorageType.SCHEMA)
                .collect(Collectors.toConcurrentMap(i -> i.getKey().getSchema(), Map.Entry::getValue));
    }

    public synchronized ConcurrentMap<String, IKeyValueStorage> getCurrentTables(String schemaName) {
        return itemToStorage.entrySet().stream()
                .filter(entry -> entry.getKey().getType() == StorageProperties.StorageType.TABLE
                        && entry.getKey().schema != null
                        && entry.getKey().schema.equals(schemaName))
                .collect(Collectors.toConcurrentMap(i -> i.getKey().getTable(), Map.Entry::getValue));
    }

    private synchronized void initRoot() throws DataStorageException, IOException {
        IKeyValueStorage storage = createStorage(rootDirectory);
        FileItem fileItem = new FileItem(null, null, StorageProperties.StorageType.ROOT);
        itemToStorage.put(fileItem, storage);
    }

    private synchronized void initSchemas() throws DataStorageException {
        List<Exception> exceptionList = new ArrayList<>();
        itemToStorage.entrySet().stream()
                .filter(entry -> entry.getKey().getType() == StorageProperties.StorageType.ROOT)
                .forEach(storage -> {
                    try {
                        storage.getValue().getValues(x -> true).values().forEach(value -> {
                            DSQLSchema schemaValue = ((DSQLSchema) value);
                            StorageProperties storageProperties = new StorageProperties(schemaValue.getSchemaName(),
                                    rootDirectory.getPath() + "/" + schemaValue.getSchemaName());
                            IKeyValueStorage keyValueStorage = storageSupplier.apply(storageProperties);
                            try {
                                keyValueStorage.init();
                            } catch (Exception exception) {
                                exceptionList.add(exception);
                            }
                            FileItem fileItem = new FileItem(
                                    storageProperties.getName(),
                                    null,
                                    StorageProperties.StorageType.SCHEMA);
                            itemToStorage.put(fileItem, keyValueStorage);
                        });
                    } catch (DataStorageException dataStorageException) {
                        exceptionList.add(dataStorageException);
                    }
                });
        Exception exception = exceptionList.stream().reduce((a, b) -> {
            a.addSuppressed(b);
            return a;
        }).orElse(null);

        if (exception != null) {
            throw new DataStorageException(exception.getMessage());
        }
    }

    private synchronized void initTables() throws DataStorageException {
        List<Exception> exceptionList = new ArrayList<>();
        itemToStorage.entrySet().stream()
                .filter(entry -> entry.getKey().getType() == StorageProperties.StorageType.SCHEMA)
                .forEach(storage -> {
                    try {
                        storage.getValue().getValues(x -> true).values().forEach(value -> {
                            DSQLTable tableValue = ((DSQLTable) value);
                            StorageProperties storageProperties = new StorageProperties(tableValue.getTableName(),
                                    rootDirectory.getPath() + "/" + storage.getKey().getSchema() + "/" + tableValue.getTableName());
                            IKeyValueStorage tableStorageByDrive = storageSupplier.apply(storageProperties);
                            try {
                                tableStorageByDrive.init();
                            } catch (Exception exception) {
                                exceptionList.add(exception);
                                return;
                            }
                            FileItem fileItem = new FileItem(
                                    storage.getKey().getSchema(),
                                    storageProperties.getName(),
                                    StorageProperties.StorageType.TABLE);
                            itemToStorage.put(fileItem, tableStorageByDrive);
                        });
                    } catch (DataStorageException dataStorageException) {
                        exceptionList.add(dataStorageException);
                    }
                });

        Exception exception = exceptionList.stream().reduce((a, b) -> {
            a.addSuppressed(b);
            return a;
        }).orElse(null);

        if (exception != null) {
            throw new DataStorageException(exception.getMessage());
        }
    }

    private IKeyValueStorage createStorage(StorageProperties storageProperties) throws DataStorageException, IOException {
        IKeyValueStorage storage = storageSupplier.apply(storageProperties);
        storage.init();
        return storage;
    }

    @Override
    public synchronized void init() throws DataStorageException, IOException {
        if (isInit()) {
            return;
        }

        recoverFromDirectories();
        logInit();
        setInit();
    }

    private void recoverFromDirectories() throws DataStorageException, IOException {
        initRoot();
        initSchemas();
        initTables();
    }

    @Override
    public void run() {
        synchronized (this) {
            if (isRunning()) {
                return;
            }

            runStorages();

            setRunning();
            logRunning();
        }

        executorService.shutdown();
        joinExecutorService(executorService);
    }

    private void runStorages() {
        itemToStorage.values().forEach(executorService::submit);
    }

    @Override
    public void close() throws Exception {
        synchronized (this) {
            if (isClosed()) {
                return;
            }

            setClosed();
            logClose();
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

    private class FileItem {
        private StorageProperties.StorageType type;
        private String schema;
        private String table;

        public FileItem(String schema, String table, StorageProperties.StorageType type) {
            this.schema = schema;
            this.table = table;
            this.type = type;
        }

        public String getSchema() {
            return schema;
        }

        public String getTable() {
            return table;
        }

        public StorageProperties.StorageType getType() {
            return type;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FileItem fileItem = (FileItem) o;
            return type == fileItem.type &&
                    Objects.equals(schema, fileItem.schema) &&
                    Objects.equals(table, fileItem.table);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, schema, table);
        }
    }
}