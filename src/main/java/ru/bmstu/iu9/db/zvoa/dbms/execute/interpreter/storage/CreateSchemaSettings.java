package ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage;

public class CreateSchemaSettings {
    private final String schemaName;
    private final String schemaPath;

    private CreateSchemaSettings(Builder builder) {
        this.schemaName = builder.schemaName;
        this.schemaPath = builder.schemaPath;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public static class Builder {
        private String schemaName;
        private String schemaPath;

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder setSchemaPath(String schemaPath) {
            this.schemaPath = schemaPath;
            return this;
        }

        public Builder setSchemaName(String schemaName) {
            this.schemaName = schemaName;
            return this;
        }

        public CreateSchemaSettings build() {
            return new CreateSchemaSettings(this);
        }
    }
}
