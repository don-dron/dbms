package ru.bmstu.iu9.db.zvoa.dbms.execute.compiler.optimizer;

import ru.bmstu.iu9.db.zvoa.dbms.execute.IProgram;

public interface IOptimizer {
    public IProgram optimize(IProgram program);
}
