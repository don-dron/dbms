package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.storage.driver.LSMStore;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.storage.memory.DSQLSchema;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.storage.memory.DSQLTable;
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
    private final String directory;
    private ConcurrentSkipListSet<Schema> schemas = new ConcurrentSkipListSet<>(Comparator.comparingInt(Schema::hashCode));

    private DBMSDataStorage(Builder builder) {
        assert (builder.directory != null);
        this.directory = builder.directory;
    }

    @Override
    public synchronized Schema createSchema(CreateSchemaSettings settings) throws DataStorageException {
        Schema schema = DSQLSchema.Builder.newBuilder()
                .setSchemaName(settings.getSchemaName())
                .build();

        if (schemas.add(schema)) {
            return schema;
        } else {
            throw new DataStorageException("Schema already exist.");
        }
    }

    @Override
    public synchronized Table createTable(CreateTableSettings settings) throws DataStorageException {
        DSQLTable dsqlTable = (DSQLTable) getSchema(settings.getSchemaName()).createTable(settings);

        try {
            LSMStore lsmStore = new LSMStore(Path.of(directory + "/" + settings.getTableName()));
            dsqlTable.setLsmStore(lsmStore);
        } catch (IOException ioException) {
            logger.error("Cannot create files for table on driver: " + ioException.getMessage());
            ioException.printStackTrace();
        }

        return dsqlTable;
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
        private String directory;

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder setDirectory(String directory) {
            this.directory = directory;
            return this;
        }

        public DBMSDataStorage build() {
            return new DBMSDataStorage(this);
        }
    }
}
