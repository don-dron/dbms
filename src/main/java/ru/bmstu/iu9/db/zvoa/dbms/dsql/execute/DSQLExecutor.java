package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute;

import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.compiler.DSQLCompiler;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.vm.DSQLVM;
import ru.bmstu.iu9.db.zvoa.dbms.execute.CompilationError;
import ru.bmstu.iu9.db.zvoa.dbms.execute.IExecutor;
import ru.bmstu.iu9.db.zvoa.dbms.execute.IProgram;
import ru.bmstu.iu9.db.zvoa.dbms.execute.RuntimeError;
import ru.bmstu.iu9.db.zvoa.dbms.execute.compiler.ICompiler;
import ru.bmstu.iu9.db.zvoa.dbms.execute.vm.IVM;

public class DSQLExecutor implements IExecutor {

    private ICompiler compiler = new DSQLCompiler();
    private IVM vm = new DSQLVM();

    @Override
    public String execute(String string) throws CompilationError, RuntimeError {
        IProgram program = compiler.compile(string);
        String result = vm.run(program);
        return result;
    }
}
