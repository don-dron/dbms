package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.engine;

import ru.bmstu.iu9.db.zvoa.dbms.execute.RuntimeError;

public interface IEngine<T> {
    public void execute(T t) throws RuntimeError;
}
