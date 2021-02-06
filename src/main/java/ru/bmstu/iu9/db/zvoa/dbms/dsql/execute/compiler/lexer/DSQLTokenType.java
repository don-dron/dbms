package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.compiler.lexer;

import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.compiler.ActionInterface;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.compiler.GrammaticalInterface;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

public enum DSQLTokenType implements GrammaticalInterface {
    // The order of the tokens implies in precedence
    CREATE(),
    DATABASE(),
    ALTER(),
    ADD(),
    RENAME(),
    MODIFY(),
    DROP(),
    TRUNCATE(),
    TABLE(),
    SELECT(),
    DELETE(),
    INSERT(),
    INTO(),
    VALUES(),
    WHERE(),
    FROM(),
    USE(),
    NOT(),
    NULL(),
    PRIMARY(),
    FOREIGN(),
    KEY(),
    AUTO_INCREMENT(),
    REFERENCES(),
    OPEN_PARENTHESIS(),
    CLOSE_PARENTHESIS(),
    QUOTE(),
    OPERATOR(),
    IN(),
    LOGICAL(),
    STAR(),
    COMMA(),
    END_STATEMENT(),
    EOF(),
    date(),
    NUMERIC(),
    CHAR(),
    VARCHAR(),
    integer(),
    number(),
    id(),
    ERROR();

    public ActionInterface action = null;

    private Map<String, String> attrs;

    public boolean isTokenType() {
        return true;
    }

    public boolean isGrammar() {
        return false;
    }

    public boolean isAction() {
        return false;
    }

    public boolean isSynthesized() {
        return false;
    }

    public String getAttr(String key) {
        return attrs.get(key);
    }

    public void cleanAttrs() {
        attrs = new HashMap<String, String>();
    }

    public void setAttr(String key, String value) {
        attrs.put(key, value);
    }

    public void getFromParent(GrammaticalInterface parent) {
        if (action != null)
            action.getFromParent(parent, attrs);
    }

    public void act(Stack<GrammaticalInterface> stack) {
        if (action != null)
            action.act(stack, attrs);
    }

    private DSQLTokenType(String... attrs) {
        this.attrs = new HashMap<String, String>();

        for (String attr : attrs) {
            setAttr(attr, null);
        }
    }
}