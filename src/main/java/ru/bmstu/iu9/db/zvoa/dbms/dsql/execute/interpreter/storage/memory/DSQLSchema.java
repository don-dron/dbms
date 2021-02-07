package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.storage.memory;

import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Schema;

public class DSQLSchema extends Schema {
    private DSQLSchema(Builder builder) {
        super(builder.schemaName);
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

        public DSQLSchema build() {
            return new DSQLSchema(this);
        }
    }
}
