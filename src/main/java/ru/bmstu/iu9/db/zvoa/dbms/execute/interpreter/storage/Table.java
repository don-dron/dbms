package ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage;

import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.storage.driver.DBMSDriverStorage;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.storage.memory.DBMSInMemoryStorage;

import java.util.List;

public class Table {
    private List<Type> types;

    public void getRow(Object key) {

    }

    public void putRow(List<Object> key) {

    }

    public void removeRow(Object key) {

    }

    private Table(Builder builder) {

    }

    public class Row {
        private Table table;
        private List<Object> values;

        private Row(Table table, List<Object> values) {
            this.table = table;
            this.values = values;
        }
    }

    public static class Builder {
        private String name;
        private List<Type> types;

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setTypes(List<Type> types) {
            this.types = types;
            return this;
        }

        public Table build() {
            return new Table(this);
        }
    }
}
