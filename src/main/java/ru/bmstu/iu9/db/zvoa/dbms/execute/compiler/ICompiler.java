package ru.bmstu.iu9.db.zvoa.dbms.execute.compiler;

import ru.bmstu.iu9.db.zvoa.dbms.execute.CompilationError;
import ru.bmstu.iu9.db.zvoa.dbms.execute.IProgram;

public interface ICompiler {
    public IProgram compile(String program) throws CompilationError;
}
