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
package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.FileSystemManager;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.memory.DSQLSchema;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.memory.DSQLTable;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.*;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.*;
import ru.bmstu.iu9.db.zvoa.dbms.modules.AbstractDbModule;
import ru.bmstu.iu9.db.zvoa.dbms.modules.IDbModule;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;

public class DBMSDataStorage extends AbstractDbModule implements DataStorage {
    private final Logger logger = LoggerFactory.getLogger(DBMSDataStorage.class);
    private final FileSystemManager fileSystemManager;
    private IKeyValueStorage<SchemeIdentification, DSQLSchema> fileSchemasStorage;
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

        executorService.shutdown();
        joinExecutorService(executorService);

        linkStorages();

        printLog();
        logInit();
        setInit();
    }

    private void printLog() throws DataStorageException {
        System.out.println("Initialization storage debug.");
        System.out.println("__________________________________________________________");

        for (Schema schema : schemas) {
            System.out.println(schema.getSchemaName());

            for (Table table : schema.getTables()) {
                System.out.println(schema.getSchemaName() + "." + table.getTableName());
                List<Row> rows = table.selectRows(SelectSettings.Builder.newBuilder().build());
                System.out.println("Rows: " + rows.size());
                for (Row row : rows) {
                    System.out.println(row);
                }
            }
        }
        System.out.println("__________________________________________________________");
    }

    @Override
    public void run() {
        ExecutorService executorService;
        synchronized (this) {
            if (isRunning() || isClosed()) {
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
        printLog();
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
                if (executorService.awaitTermination(100, TimeUnit.MILLISECONDS)) {
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
                        throw new IllegalStateException(exception.getMessage());
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
                                String tableName = tableEntry.getKey();

                                try {
                                    DSQLTable tableSource = (DSQLTable) storage.get(new TableIdentification(tableName));
                                    IKeyValueStorage tableStorage = tableEntry.getValue();
                                    DSQLTable table = DSQLTable.Builder.newBuilder()
                                            .setName(tableName)
                                            .setTypes(tableSource.getTypes())
                                            .setRowToKey(tableSource.getRowKeyFunction())
                                            .setStorage(tableStorage)
                                            .build();
                                    schema.addTable(table);
                                } catch (DataStorageException dataStorageException) {
                                    throw new IllegalStateException(dataStorageException.getMessage());
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
                    .setTypes(settings.getTypes())
                    .build();
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
            fileSchemasStorage.put(new SchemeIdentification(schema.getSchemaName()), schema);
            schemas.add(schema);
            return schema;
        }
    }

    public Table deleteTable(CreateTableSettings settings) throws Exception {
        Schema schema = useSchema(settings.getSchemaName());
        Table table = schema.useTable(settings.getTableName());
        IKeyValueStorage tableStorage = fileSystemManager.getCurrentTables(settings.getSchemaName()).get(table.getTableName());
        tableStorage.close();
        schema.deleteTable(table);
        return table;
    }

    public Schema deleteSchema(DeleteSchemaSettings settings) throws Exception {
        Schema schema = useSchema(settings.getSchemaName());
        IKeyValueStorage schemaStorage = fileSystemManager.getCurrentSchemas().get(schema.getSchemaName());
        schemaStorage.close();
        fileSchemasStorage.put(new SchemeIdentification(schema.getSchemaName()), null);
        schemas.remove(schema);
        return schema;
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
