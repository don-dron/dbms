package ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage;

import java.util.List;

public class DeleteTableSettings {
    private final String schemaName;
    private final String tableName;

    private DeleteTableSettings(Builder builder) {
        this.schemaName = builder.schemaName;
        this.tableName = builder.tableName;
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
            return new DeleteTableSettings.Builder();
        }

        public Builder setSchemaName(String schemaName) {
            this.schemaName = schemaName;
            return this;
        }

        public Builder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public DeleteTableSettings build() {
            return new DeleteTableSettings(this);
        }
    }
}
