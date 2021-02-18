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
package ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage;

import java.util.List;

public class CreateTableSettings {
    private final String schemaName;
    private final String tableName;
    private final String tablePath;
    private final List<Type> keysTypes;
    private final List<Type> valuesTypes;

    private CreateTableSettings(Builder builder) {
        this.schemaName = builder.schemaName;
        this.tableName = builder.tableName;
        this.tablePath = builder.tablePath;
        this.keysTypes = builder.keysTypes;
        this.valuesTypes = builder.valuesTypes;
    }

    public String getTablePath() {
        return tablePath;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public List<Type> getKeyTypes() {
        return valuesTypes;
    }

    public List<Type> getValuesTypes() {
        return valuesTypes;
    }

    public static class Builder {
        private String schemaName;
        private String tableName;
        private String tablePath;
        private List<Type> keysTypes;
        private List<Type> valuesTypes;

        public static Builder newBuilder() {
            return new CreateTableSettings.Builder();
        }

        public Builder setTablePath(String tablePath) {
            this.tablePath = tablePath;
            return this;
        }

        public Builder setSchemaName(String schemaName) {
            this.schemaName = schemaName;
            return this;
        }

        public Builder setTypes(List<Type> types) {
            this.valuesTypes = types;
            return this;
        }

        public Builder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public CreateTableSettings build() {
            return new CreateTableSettings(this);
        }
    }
}
