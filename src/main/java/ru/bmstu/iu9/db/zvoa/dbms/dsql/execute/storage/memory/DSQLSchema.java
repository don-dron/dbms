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
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorageException;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Schema;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Table;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.TableIdentification;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

public class DSQLSchema extends Schema {
    private transient IKeyValueStorage<TableIdentification, Table> storage;
    private transient ConcurrentSkipListSet<Table> tables;

    public DSQLSchema(Builder builder) {
        super(builder.schemaName, builder.schemaPath);
        storage = builder.storage;
        tables = builder.tables;
    }

    @Override
    public List<Table> getTables() {
        return Arrays.asList(tables.toArray(Table[]::new));
    }

    @Override
    public boolean addTable(Table table) throws DataStorageException {
        if (tables.add(table)) {
            storage.put(new TableIdentification(table.getTableName()), table);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean deleteTable(Table table) throws DataStorageException {
        if (tables.remove(table)) {
            storage.put(new TableIdentification(table.getTableName()), null);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public synchronized Table getTable(String tableName) throws DataStorageException {
        return tables.stream()
                .filter(table -> table.getTableName().equals(tableName))
                .findFirst()
                .orElse(null);
    }

    @Override
    public synchronized Table useTable(String tableName) throws DataStorageException {
        return tables.stream()
                .filter(table -> table.getTableName().equals(tableName))
                .findFirst()
                .orElseThrow(() -> new DataStorageException("Table " + tableName + " not found"));
    }

    public void setStorage(IKeyValueStorage schemaStorage) {
        this.storage = schemaStorage;
    }

    public static class Builder {
        private IKeyValueStorage storage;
        private String schemaName;
        private String schemaPath;
        private ConcurrentSkipListSet<Table> tables = new ConcurrentSkipListSet<>(Comparator.comparingInt(Table::hashCode));

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder setStorage(IKeyValueStorage storage) {
            this.storage = storage;
            return this;
        }

        public Builder setSchemaName(String schemaName) {
            this.schemaName = schemaName;
            return this;
        }

        public Builder setSchemaPath(String schemaPath) {
            this.schemaPath = schemaPath;
            return this;
        }

        public Builder setTables(Set<Table> tables) {
            this.tables = new ConcurrentSkipListSet<>(tables);
            return this;
        }

        public DSQLSchema build() {
            return new DSQLSchema(this);
        }
    }
}
