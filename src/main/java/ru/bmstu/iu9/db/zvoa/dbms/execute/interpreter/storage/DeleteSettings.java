package ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage;

import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Table;

import java.util.List;

public class DeleteSettings {
    private final String schemaName;
    private final String tableName;
    private final List<List<Object>> rows;

    private DeleteSettings(DeleteSettings.Builder builder) {
        this.tableName = builder.tableName;
        this.schemaName = builder.schemaName;
        this.rows = builder.rows;
    }

    public List<List<Object>> getRows() {
        return rows;
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
        private List<List<Object>> rows;

        public static Builder newBuilder() {
            return new DeleteSettings.Builder();
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

        public DeleteSettings build() {
            return new DeleteSettings(this);
        }
    }
}
