package ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage;

import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.storage.driver.LSMStore;

import java.util.List;

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
