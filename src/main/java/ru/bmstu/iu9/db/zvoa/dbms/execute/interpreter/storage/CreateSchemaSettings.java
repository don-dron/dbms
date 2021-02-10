package ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage;

public class CreateSchemaSettings {
    private final String schemaName;

    private CreateSchemaSettings(Builder builder) {
        this.schemaName = builder.schemaName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public static class Builder {
        private String schemaName;
        public static Builder newBuilder() {
            return new Builder();
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
