package ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage;

public class InsertSettings {
    private final String schemaName;
    private final String tableName;

    private InsertSettings(InsertSettings.Builder builder) {
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

        public InsertSettings build() {
            return new InsertSettings(this);
        }
    }
}
