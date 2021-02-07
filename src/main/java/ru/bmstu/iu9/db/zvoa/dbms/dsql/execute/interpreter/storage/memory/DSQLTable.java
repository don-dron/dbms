package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.storage.memory;

import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DeleteSettings;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.InsertSettings;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.SelectSettings;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.Type;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Table;

import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

public class DSQLTable extends Table {
    private final ConcurrentSkipListMap<Object, Row> rowMap;

    private DSQLTable(Builder builder) {
        super(builder.name, builder.types);
        rowMap = new ConcurrentSkipListMap<>();
    }

    public List<Row> selectRows(SelectSettings selectSettings) {
        return rowMap.values().stream().collect(Collectors.toList());
    }

    public List<Row> insertRows(InsertSettings insertSettings) {
        return rowMap.values().stream().collect(Collectors.toList());
    }

    public List<Row> deleteRows(DeleteSettings deleteSettings) {
        return rowMap.values().stream().collect(Collectors.toList());
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

        public DSQLTable build() {
            return new DSQLTable(this);
        }
    }
}
