package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.compiler;

import java.util.*;

public class GrammaticalElement implements GrammaticalInterface {
    private boolean tokenType;
    private boolean grammar;
    private boolean synthesized;

    private Map<String, String> attrs;
    public ActionInterface action;

    public boolean isTokenType() {
        return tokenType;
    }

    public boolean isGrammar() {
        return grammar;
    }

    public boolean isAction () {
        return action != null;
    }

    public boolean isSynthesized () {
        return synthesized;
    }

    public String getAttr(String key) {
        return attrs.get(key);
    }

    public void setAttr(String key, String value) {
        attrs.put(key, value);
    }

    public void act(Stack<GrammaticalInterface> stack) {
        action.act(stack, attrs);
    }

    public void getFromParent(GrammaticalInterface parent) {
        action.getFromParent(parent, attrs);
    }

    public String name() {
        return getClass().getSimpleName();
    }

    public void cleanAttrs() {
        attrs = new HashMap<String, String>();
    }

    public GrammaticalElement (){
        attrs = new HashMap<String, String>();
        this.action = null;
    }

    public GrammaticalElement (ActionInterface action){
        attrs = new HashMap<String, String>();
        this.action = action;
    }
}