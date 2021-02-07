package ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage;

import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Table;

import java.util.List;

public class InsertSettings {
    private final String schemaName;
    private final String tableName;
    private final List<List<Object>> rows;

    private InsertSettings(InsertSettings.Builder builder) {
        this.tableName = builder.tableName;
        this.schemaName = builder.schemaName;
        this.rows = builder.rows;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public List<List<Object>> getRows() {
        return rows;
    }

    public static class Builder {
        private String schemaName;
        private String tableName;
        private List<List<Object>> rows;

        public static Builder newBuilder() {
            return new InsertSettings.Builder();
        }

        public Builder setSchemaName(String schemaName) {
            this.schemaName = schemaName;
            return this;
        }

        public Builder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder setRows(List<List<Object>> rows) {
            this.rows = rows;
            return this;
        }

        public InsertSettings build() {
            return new InsertSettings(this);
        }
    }
}
