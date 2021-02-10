package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.memory;

import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.IKeyValueStorage;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Value;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.*;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Table;

import java.util.List;
import java.util.stream.Collectors;

public class DSQLTable extends Table {
    private final IKeyValueStorage storage;

    private DSQLTable(Builder builder) {
        super(builder.name, builder.types);
        storage = builder.storage;
    }

    public List<Row> selectRows(SelectSettings selectSettings) throws DataStorageException {
        if (storage == null) {
            throw new DataStorageException("Driver store not connected");
        } else {
            List<String> result = storage.getValues((string) -> true).stream().map(Value::toString).collect(Collectors.toList());
            return result.stream().map(raw -> Row.parseString(this, raw)).collect(Collectors.toList());
        }
    }

    public List<Row> insertRows(InsertSettings insertSettings) throws DataStorageException {
        if (storage == null) {
            throw new DataStorageException("Driver store not connected");
        } else {
            List<Row> rows = insertSettings.getRows().stream().map(this::createRow).collect(Collectors.toList());
            for (Row row : rows) {
//                    lsmStore.put(new KVItem(row.getKey().toString(), row.toString(), Instant.now().toEpochMilli()));
            }
            return rows;
        }
    }

    public List<Row> deleteRows(DeleteSettings deleteSettings) throws DataStorageException {
        if (storage == null) {
            throw new DataStorageException("Driver store not connected");
        } else {
            List<Row> rows = deleteSettings.getRows().stream().map(this::createRow).collect(Collectors.toList());
            for (Row row : rows) {
//                    lsmStore.put(new KVItem(row.getKey().toString(), null, Instant.now().toEpochMilli()));
            }
            return rows;
        }
    }

    protected Row createRow(List<Object> values) {
        return new Row(this, values);
    }

    public static class Builder {
        private String name;
        private List<Type> types;
        private IKeyValueStorage storage;

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setStorage(IKeyValueStorage storage) {
            this.storage = storage;
            return this;
        }

        public Builder setTypes(List<Type> types) {
            this.types = types;
            return this;
        }

        public DSQLTable build() {
            return new DSQLTable(this);
        }
    }
}
