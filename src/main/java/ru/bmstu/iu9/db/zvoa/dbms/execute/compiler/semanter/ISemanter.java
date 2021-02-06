package ru.bmstu.iu9.db.zvoa.dbms.execute.compiler.semanter;

import ru.bmstu.iu9.db.zvoa.dbms.execute.compiler.parser.ASTNode;

public interface ISemanter {
    public void checkSemantic(ASTNode astNode) throws SemanticError;
}
