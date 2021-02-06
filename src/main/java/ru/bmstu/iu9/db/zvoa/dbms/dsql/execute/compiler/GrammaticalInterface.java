package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.compiler;

import java.util.Stack;

public interface GrammaticalInterface {
    public boolean isTokenType();

    public boolean isGrammar();

    public boolean isAction();

    public boolean isSynthesized();

    void setAttr(String key, String value);

    String getAttr(String key);

    String name();

    void cleanAttrs();

    void act(Stack<GrammaticalInterface> stack);

    void getFromParent(GrammaticalInterface parent);
}