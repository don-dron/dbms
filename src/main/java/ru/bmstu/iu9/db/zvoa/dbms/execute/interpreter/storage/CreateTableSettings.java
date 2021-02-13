package ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage;

import java.util.List;

public class CreateTableSettings {
    private final String schemaName;
    private final String tableName;
    private final String tablePath;
    private final List<Type> types;

    private CreateTableSettings(Builder builder) {
        this.schemaName = builder.schemaName;
        this.tableName = builder.tableName;
        this.tablePath = builder.tablePath;
        this.types = builder.types;
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

    public List<Type> getTypes() {
        return types;
    }

    public static class Builder {
        private String schemaName;
        private String tableName;
        private String tablePath;
        private List<Type> types;

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
            this.types = types;
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
