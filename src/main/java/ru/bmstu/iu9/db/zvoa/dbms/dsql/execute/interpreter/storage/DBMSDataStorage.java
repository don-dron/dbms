package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.storage.driver.DBMSDriverStorage;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.storage.memory.DBMSInMemoryStorage;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.*;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Schema;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Table;

import java.util.List;

public class DBMSDataStorage implements DataStorage {
    private final Logger logger = LoggerFactory.getLogger(DBMSDataStorage.class);

    private final DBMSInMemoryStorage inMemoryStorage;
    private final DBMSDriverStorage driverStorage;
    private final boolean driverStorageEnabled;

    private DBMSDataStorage(Builder builder) {
        this.inMemoryStorage = builder.inMemoryStorage;
        this.driverStorage = builder.driverStorage;
        this.driverStorageEnabled = builder.driverStorageEnabled;
    }

    @Override
    public Schema createSchema(CreateSchemaSettings settings) throws DataStorageException {
        return inMemoryStorage.createSchema(settings);
    }

    @Override
    public Table createTable(CreateTableSettings createTableSettings) throws DataStorageException {
        return inMemoryStorage.createTable(createTableSettings);
    }

    @Override
    public List<Table.Row> insertRows(InsertSettings insertSettings) throws DataStorageException {
        return inMemoryStorage.insertRows(insertSettings);
    }

    @Override
    public List<Table.Row> selectRows(SelectSettings selectSettings) throws DataStorageException {
        return inMemoryStorage.selectRows(selectSettings);
    }

    @Override
    public List<Table.Row> deleteRows(DeleteSettings deleteSettings) throws DataStorageException {
        return inMemoryStorage.deleteRows(deleteSettings);
    }

    public static class Builder {
        private DBMSInMemoryStorage inMemoryStorage;
        private DBMSDriverStorage driverStorage;
        private boolean driverStorageEnabled;

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder setDriverStorage(DBMSDriverStorage driverStorage) {
            this.driverStorage = driverStorage;
            return this;
        }

        public Builder setInMemoryStorage(DBMSInMemoryStorage inMemoryStorage) {
            this.inMemoryStorage = inMemoryStorage;
            return this;
        }

        public Builder useDriverStorage() {
            this.driverStorageEnabled = true;
            return this;
        }

        public DBMSDataStorage build() {
            return new DBMSDataStorage(this);
        }
    }
}
