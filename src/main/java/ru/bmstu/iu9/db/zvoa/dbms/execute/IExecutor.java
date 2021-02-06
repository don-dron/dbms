package ru.bmstu.iu9.db.zvoa.dbms.execute;

public interface IExecutor {
    public String execute(String string) throws CompilationError, RuntimeError;
}
