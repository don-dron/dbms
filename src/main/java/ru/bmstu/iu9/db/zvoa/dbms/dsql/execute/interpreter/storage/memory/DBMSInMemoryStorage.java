package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.storage.memory;

import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.*;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Schema;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Table;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;

public class DBMSInMemoryStorage implements DataStorage {
    private ConcurrentSkipListSet<Schema> schemas = new ConcurrentSkipListSet<>(Comparator.comparingInt(Schema::hashCode));

    @Override
    public synchronized Schema createSchema(CreateSchemaSettings settings) throws DataStorageException {
        Schema schema = DSQLSchema.Builder.newBuilder()
                .setSchemaName(settings.getSchemaName())
                .build();

        if (schemas.add(schema)) {
            return schema;
        } else {
            throw new DataStorageException("Schema already exist.");
        }
    }

    @Override
    public synchronized Table createTable(CreateTableSettings settings) throws DataStorageException {
        return getSchema(settings.getSchemaName()).createTable(settings);
    }

    @Override
    public synchronized List<Table.Row> insertRows(InsertSettings insertSettings) throws DataStorageException {
        return getSchema(insertSettings.getSchemaName())
                .getTable(insertSettings.getTableName())
                .insertRows(insertSettings);
    }

    @Override
    public synchronized List<Table.Row> selectRows(SelectSettings selectSettings) throws DataStorageException {
        return getSchema(selectSettings.getSchemaName())
                .getTable(selectSettings.getTableName())
                .selectRows(selectSettings);
    }

    @Override
    public synchronized List<Table.Row> deleteRows(DeleteSettings deleteSettings) throws DataStorageException {
        return getSchema(deleteSettings.getSchemaName())
                .getTable(deleteSettings.getTableName())
                .deleteRows(deleteSettings);
    }

    private Schema getSchema(String schemaName) throws DataStorageException {
        Schema schema = schemas.stream()
                .filter(sch -> sch.getSchemaName().equals(schemaName))
                .findFirst()
                .orElseThrow(() -> new DataStorageException("Undefined schema " + schemaName));
        return schema;
    }
}
