package ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage;

public class SelectSettings {
    private final String schemaName;
    private final String tableName;

    private SelectSettings(Builder builder) {
        this.tableName = builder.tableName;
        this.schemaName = builder.schemaName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public static class Builder {
        private String schemaName;
        private String tableName;

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder setSchemaName(String schemaName) {
            this.schemaName = schemaName;
            return this;
        }

        public Builder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public SelectSettings build() {
            return new SelectSettings(this);
        }
    }
}