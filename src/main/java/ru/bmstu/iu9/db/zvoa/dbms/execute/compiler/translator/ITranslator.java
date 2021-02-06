package ru.bmstu.iu9.db.zvoa.dbms.execute.compiler.translator;

import ru.bmstu.iu9.db.zvoa.dbms.execute.IProgram;
import ru.bmstu.iu9.db.zvoa.dbms.execute.compiler.parser.ASTNode;

public interface ITranslator {
    public IProgram translate(ASTNode astNode);
}
