package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.engine;

import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorage;

public abstract class DSQLEngine<T> implements IEngine<T> {
    protected final DataStorage dataStorage;

    public DSQLEngine(DataStorage dataStorage) {
        this.dataStorage = dataStorage;
    }
}
