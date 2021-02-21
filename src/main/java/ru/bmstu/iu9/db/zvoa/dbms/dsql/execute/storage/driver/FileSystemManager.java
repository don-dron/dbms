/*
 * Copyright (c) 2021 Zvorygin Andrey BMSTU IU-9 https://github.com/don-dron
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
 * OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT
 * OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver;

import org.jetbrains.annotations.NonNls;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.IKeyValueStorage;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.converter.SchemeConverter;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.converter.TableConverter;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.memory.DSQLSchema;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.memory.DSQLTable;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.CreateSchemaSettings;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.CreateTableSettings;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorageException;
import ru.bmstu.iu9.db.zvoa.dbms.modules.AbstractDbModule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Manager for connection virtual bases with drive storage.
 *
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public class FileSystemManager extends AbstractDbModule {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemManager.class);
    private final ExecutorService executorService;
    private final ConcurrentHashMap<FileItem, IKeyValueStorage> itemToStorage = new ConcurrentHashMap<>();
    private final StorageProperties rootDirectory;
    private final Function<StorageProperties, IKeyValueStorage> storageSupplier;

    /**
     * Constructor by config.
     *
     * @param fileSystemManagerConfig - config
     */
    public FileSystemManager(@NotNull FileSystemManagerConfig fileSystemManagerConfig) {
        rootDirectory = fileSystemManagerConfig.getStorageProperties();
        storageSupplier = fileSystemManagerConfig.getStorageSupplier();
        executorService = createExecutorService();
    }

    public synchronized IKeyValueStorage createTableStorage(@NotNull DSQLTable table,
                                                            @NotNull CreateTableSettings settings) throws DataStorageException, IOException {
        StorageProperties storageProperties = new StorageProperties(
                new TableConverter(table, Arrays.asList(table.getKeyType()), table.getTypes()),
                settings.getTableName(),
                rootDirectory.getPath() + "/" + settings.getSchemaName() + "/" + settings.getTableName());
        IKeyValueStorage storage = storageSupplier.apply(storageProperties);
        FileItem fileItem = new FileItem(settings.getSchemaName(), settings.getTableName(), StorageProperties.StorageType.TABLE);
        storage.init();
        if (isRunning()) {
            executorService.submit(storage);
        }
        itemToStorage.put(fileItem, storage);
        return storage;
    }

    public synchronized IKeyValueStorage createSchemaStorage(DSQLSchema schema,
                                                             CreateSchemaSettings settings) throws DataStorageException, IOException {
        StorageProperties storageProperties = new StorageProperties(
                new SchemeConverter(),
                settings.getSchemaName(),
                rootDirectory.getPath() + "/" + settings.getSchemaName());
        IKeyValueStorage storage = storageSupplier.apply(storageProperties);
        FileItem fileItem = new FileItem(settings.getSchemaName(), null, StorageProperties.StorageType.SCHEMA);
        storage.init();
        if (isRunning()) {
            executorService.submit(storage);
        }
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

    public synchronized ConcurrentMap<String, IKeyValueStorage> getCurrentTables(@NonNls String schemaName) {
        return itemToStorage.entrySet().stream()
                .filter(entry -> entry.getKey().getType() == StorageProperties.StorageType.TABLE
                        && entry.getKey().getSchema() != null
                        && entry.getKey().getSchema().equals(schemaName))
                .collect(Collectors.toConcurrentMap(i -> i.getKey().getTable(), Map.Entry::getValue));
    }

    private synchronized void initRoot() throws DataStorageException, IOException {
        IKeyValueStorage storage = storageSupplier.apply(rootDirectory);
        FileItem fileItem = new FileItem(null, null, StorageProperties.StorageType.ROOT);
        storage.init();
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
                            StorageProperties storageProperties = new StorageProperties(new SchemeConverter(), schemaValue.getSchemaName(),
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
                            StorageProperties storageProperties = new StorageProperties(
                                    new TableConverter(tableValue,
                                            Arrays.asList(tableValue.getKeyType()),
                                            tableValue.getTypes()),
                                    tableValue.getTableName(),
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
                                    tableValue.getTableName(),
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

            if (isClosed()) {
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
    public synchronized void close() throws Exception {
        if (isClosed()) {
            return;
        }

        for (IKeyValueStorage storage : itemToStorage.values()) {
            storage.close();
        }
        setClosed();
        logClose();
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
                if (executorService.awaitTermination(0, TimeUnit.MILLISECONDS)) {
                    break;
                } else {
                    Thread.yield();
                }
            } catch (InterruptedException exception) {
                LOGGER.warn(exception.getMessage());
            }
        }
    }

    private static class FileItem {
        private final StorageProperties.StorageType type;
        private final String schema;
        private final String table;

        private FileItem(String schema, String table, StorageProperties.StorageType type) {
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
