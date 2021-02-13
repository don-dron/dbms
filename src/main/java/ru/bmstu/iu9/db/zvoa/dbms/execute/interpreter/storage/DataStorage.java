package ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage;

import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Row;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Schema;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Table;

import java.io.IOException;
import java.util.List;

public interface DataStorage {
    public Schema createSchema(CreateSchemaSettings settings) throws DataStorageException, IOException;

    public Table createTable(CreateTableSettings createTableSettings) throws DataStorageException, IOException;

    public List<Row> insertRows(InsertSettings insertSettings) throws DataStorageException;

    public List<Row> selectRows(SelectSettings selectSettings) throws DataStorageException;

    public List<Row> deleteRows(DeleteSettings deleteSettings) throws DataStorageException;
}
