package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.compiler.lexer;

import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.compiler.GrammaticalInterface;
import ru.bmstu.iu9.db.zvoa.dbms.execute.compiler.lexer.IToken;

public class DSQLToken implements IToken {
    public GrammaticalInterface type;
    public String data;

    public DSQLToken(GrammaticalInterface type, String data) {
        this.type = type;
        this.data = data;
    }

    @Override
    public String toString() {
        return String.format("(%s %s)", type.name(), data);
    }
}
