package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.compiler;

import java.util.Map;
import java.util.Stack;

public interface ActionInterface {
    public void act(Stack<GrammaticalInterface> stack, Map<String, String> attrs);

    public void getFromParent(GrammaticalInterface parent, Map<String, String> attrs);
}