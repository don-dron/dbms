package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.vm;

import ru.bmstu.iu9.db.zvoa.dbms.execute.IProgram;
import ru.bmstu.iu9.db.zvoa.dbms.execute.vm.IVM;

public class DSQLVM implements IVM {
    @Override
    public String run(IProgram program) {
        return "echo";
    }
}
