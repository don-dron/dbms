package ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage;

import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Schema;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.memory.Table;

import java.util.List;

public interface DataStorage {
    public Schema createSchema(CreateSchemaSettings settings) throws DataStorageException;

    public Table createTable(CreateTableSettings createTableSettings) throws DataStorageException;

    public List<Table.Row> insertRows(InsertSettings insertSettings) throws DataStorageException;

    public List<Table.Row> selectRows(SelectSettings selectSettings) throws DataStorageException;

    public List<Table.Row> deleteRows(DeleteSettings deleteSettings) throws DataStorageException;
}
