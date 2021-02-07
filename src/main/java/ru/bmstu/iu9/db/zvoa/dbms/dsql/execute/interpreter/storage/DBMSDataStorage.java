package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.storage.driver.DBMSDriverStorage;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.storage.memory.DBMSInMemoryStorage;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorage;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorageException;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.Table;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.Type;

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
    public synchronized void createTable(String tableName, List<Type> types) throws DataStorageException {

    }

    @Override
    public synchronized void put(String tableName, List<Object> values) throws DataStorageException {

    }

    @Override
    public synchronized void get(String tableName, Object key) throws DataStorageException {

    }

    @Override
    public synchronized void remove(String tableName, Object key) throws DataStorageException {

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
