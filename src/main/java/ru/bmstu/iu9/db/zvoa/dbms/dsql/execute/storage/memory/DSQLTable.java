package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.memory;

import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.LSMStore;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.shared.KVItem;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.*;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Table;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class DSQLTable extends Table {
    private final LSMStore lsmStore;

    private DSQLTable(Builder builder) {
        super(builder.name, builder.types);
        lsmStore = builder.lsmStore;
    }

    public List<Row> selectRows(SelectSettings selectSettings) throws DataStorageException {
        if (lsmStore == null) {
            throw new DataStorageException("Driver store not connected");
        } else {
            try {
                Set<String> result = lsmStore.getAllKeys((string) -> true);
                return result.stream().map(raw -> Row.parseString(this, raw)).collect(Collectors.toList());
            } catch (IOException e) {
                throw new DataStorageException("Select from driver error " + getTableName());
            }
        }
    }

    public List<Row> insertRows(InsertSettings insertSettings) throws DataStorageException {
        if (lsmStore == null) {
            throw new DataStorageException("Driver store not connected");
        } else {
            try {
                List<Row> rows = insertSettings.getRows().stream().map(this::createRow).collect(Collectors.toList());
                for (Row row : rows) {
                    lsmStore.put(new KVItem(row.getKey().toString(), row.toString(), Instant.now().toEpochMilli()));
                }
                return rows;
            } catch (IOException e) {
                throw new DataStorageException("Insert from driver error " + getTableName());
            }
        }
    }

    public List<Row> deleteRows(DeleteSettings deleteSettings) throws DataStorageException {
        if (lsmStore == null) {
            throw new DataStorageException("Driver store not connected");
        } else {
            try {
                List<Row> rows = deleteSettings.getRows().stream().map(this::createRow).collect(Collectors.toList());
                for (Row row : rows) {
                    lsmStore.put(new KVItem(row.getKey().toString(), null, Instant.now().toEpochMilli()));
                }
                return rows;
            } catch (IOException e) {
                throw new DataStorageException("Delete from driver error " + getTableName());
            }
        }
    }

    public static class Builder {
        private String name;
        private List<Type> types;
        private LSMStore lsmStore;

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setLsmStore(LSMStore lsmStore) {
            this.lsmStore = lsmStore;
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
