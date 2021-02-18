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
    private transient IKeyValueStorage<Key, Row> storage;

    public DSQLTable(Builder builder) {
        super(builder.name, builder.path, builder.types, builder.rowKeyFunction);
        storage = builder.storage;
    }

    public DSQLTable(List<Object> list) {
        super(list);
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
        private int rowKeyFunction;
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

        public Builder setRowToKey(int rowToKey) {
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
