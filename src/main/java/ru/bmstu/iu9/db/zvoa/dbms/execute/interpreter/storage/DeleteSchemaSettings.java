package ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage;

public class DeleteSchemaSettings {
    private final String schemaName;

    private DeleteSchemaSettings(Builder builder) {
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

        public DeleteSchemaSettings build() {
            return new DeleteSchemaSettings(this);
        }
    }
}
