package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.memory;

import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.IKeyValueStorage;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.Key;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.*;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Row;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Table;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DSQLTable extends Table {
    private transient final IKeyValueStorage<Key, Row> storage;

    private DSQLTable(Builder builder) {
        super(builder.name, builder.path, builder.types, builder.rowKeyFunction);
        storage = builder.storage;
    }

    public List<Row> selectRows(SelectSettings selectSettings) throws DataStorageException {
        if (storage == null) {
            throw new DataStorageException("Driver store not connected");
        } else {
            return storage.getValues(x -> true)
                    .values()
                    .stream()
                    .peek(row -> row.setTable(this))
                    .collect(Collectors.toList());
        }
    }

    public List<Row> insertRows(InsertSettings insertSettings) throws DataStorageException {
        if (storage == null) {
            throw new DataStorageException("Driver store not connected");
        } else {
            List<Row> rows = insertSettings.getRows().stream().map(this::createRow).collect(Collectors.toList());
            for (Row row : rows) {
                storage.put(row.getKey(), row);
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
                storage.put(row.getKey(), null);
            }
            return rows;
        }
    }

    protected Row createRow(List<Object> values) {
        return new Row(this, values);
    }

    public static class Builder {
        private String name;
        private String path;
        private List<Type> types;
        private Function<Row, Key> rowKeyFunction;
        private IKeyValueStorage<Key, Row> storage;

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public Builder setRowToKey(Function<Row, Key> rowToKey) {
            this.rowKeyFunction = rowToKey;
            return this;
        }

        public Builder setStorage(IKeyValueStorage<Key, Row> storage) {
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
