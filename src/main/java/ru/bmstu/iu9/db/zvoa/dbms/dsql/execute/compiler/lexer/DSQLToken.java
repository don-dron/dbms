/*
 * Copyright (c) 2021 Zvorygin Andrey BMSTU IU-9 https://github.com/don-dron
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
 * OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT
 * OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.compiler.lexer;

public enum DSQLToken {
    FOR("FOR"),
    IF("IF"),
    INDEX("INDEX"),
    PRIMARY("PRIMARY"),
    KEY("KEY"),
    DEFAULT("DEFAULT"),
    CONSTRAINT("CONSTRAINT"),
    CHECK("CHECK"),
    VIEW("VIEW"),
    CREATE("CREATE"),
    ALTER("ALTER"),
    DROP("DROP"),
    TABLE("TABLE"),
    UPDATE("UPDATE"),
    SET("SET"),
    SELECT("SELECT"),
    FROM("FROM"),
    WHERE("WHERE"),
    ORDER("ORDER"),
    BY("BY"),
    GROUP("GROUP"),
    HAVING("HAVING"),
    INSERT("INSERT"),
    INTO("INTO"),
    NULL("NULL"),
    NOT("NOT"),
    AS("AS"),
    DELETE("DELETE"),
    DISTINCT("DISTINCT"),
    UNIQUE("UNIQUE"),
    FOREIGN("FOREIGN"),
    REFERENCES("REFERENCES"),
    ALL("ALL"),
    UNION("UNION"),
    INTERSECT("INTERSECT"),
    MINUS("MINUS"),
    INNER("INNER"),
    LEFT("LEFT"),
    RIGHT("RIGHT"),
    FULL("FULL"),
    OUTER("OUTER"),
    JOIN("JOIN"),
    ON("ON"),
    SCHEMA("SCHEMA"),
    CAST("CAST"),
    COLUMN("COLUMN"),
    USE("USE"),
    DATABASE("DATABASE"),

    AND("AND"),
    OR("OR"),
    XOR("XOR"),
    CASE("CASE"),
    WHEN("WHEN"),
    THEN("THEN"),
    ELSE("ELSE"),
    END("END"),
    EXISTS("EXISTS"),
    IN("IN"),

    NEW("NEW"),
    ASC("ASC"),
    DESC("DESC"),
    IS("IS"),
    LIKE("LIKE"),
    ESCAPE("ESCAPE"),
    BETWEEN("BETWEEN"),
    VALUES("VALUES"),
    INTERVAL("INTERVAL"),

    LOCK("LOCK"),
    SOME("SOME"),
    ANY("ANY"),
    TRUNCATE("TRUNCATE"),

    // mysql
    TRUE("TRUE"),
    FALSE("FALSE"),
    LIMIT("LIMIT"),
    KILL("KILL"),
    IDENTIFIED("IDENTIFIED"),
    PASSWORD("PASSWORD"),
    DUAL("DUAL"),

    //postgresql
    WINDOW("WINDOW"),
    OFFSET("OFFSET"),
    ROW("ROW"),
    ROWS("ROWS"),
    ONLY("ONLY"),
    FIRST("FIRST"),
    NEXT("NEXT"),
    FETCH("FETCH"),
    OF("OF"),
    SHARE("SHARE"),
    NOWAIT("NOWAIT"),
    RECURSIVE("RECURSIVE"),
    TEMPORARY("TEMPORARY"),
    TEMP("TEMP"),
    UNLOGGED("UNLOGGED"),
    RESTART("RESTART"),
    IDENTITY("IDENTITY"),
    CONTINUE("CONTINUE"),
    CASCADE("CASCADE"),
    RESTRICT("RESTRICT"),
    USING("USING"),
    CURRENT("CURRENT"),
    RETURNING("RETURNING"),
    COMMENT("COMMENT"),
    OVER("OVER"),

    // oracle
    START("START"),
    PRIOR("PRIOR"),
    CONNECT("CONNECT"),
    WITH("WITH"),
    EXTRACT("EXTRACT"),
    CURSOR("CURSOR"),
    MODEL("MODEL"),
    MERGE("MERGE"),
    MATCHED("MATCHED"),
    ERRORS("ERRORS"),
    REJECT("REJECT"),
    UNLIMITED("UNLIMITED"),
    BEGIN("BEGIN"),
    EXCLUSIVE("EXCLUSIVE"),
    MODE("MODE"),
    WAIT("WAIT"),
    ADVISE("ADVISE"),
    SESSION("SESSION"),
    PROCEDURE("PROCEDURE"),
    LOCAL("LOCAL"),
    SYSDATE("SYSDATE"),
    DECLARE("DECLARE"),
    EXCEPTION("EXCEPTION"),
    GRANT("GRANT"),
    LOOP("LOOP"),
    GOTO("GOTO"),
    COMMIT("COMMIT"),
    SAVEPOINT("SAVEPOINT"),
    CROSS("CROSS"),

    // transact-sql
    TOP("TOP"),

    // hive

    EOF,
    ERROR,
    IDENTIFIER,
    HINT,
    VARIANT,
    LITERAL_INT,
    LITERAL_FLOAT,
    LITERAL_HEX,
    LITERAL_CHARS,
    LITERAL_NCHARS,

    LITERAL_ALIAS,
    LINE_COMMENT,
    MULTI_LINE_COMMENT,

    // Oracle
    BINARY_FLOAT,
    BINARY_DOUBLE,

    LPAREN("("),
    RPAREN(")"),
    LBRACE("{"),
    RBRACE("}"),
    LBRACKET("["),
    RBRACKET("]"),
    SEMI(";"),
    COMMA(","),
    DOT("."),
    DOTDOT(".."),
    DOTDOTDOT("..,"),
    EQ("="),
    GT(">"),
    LT("<"),
    BANG("!"),
    TILDE("~"),
    QUES("?"),
    COLON(":"),
    COLONEQ(":="),
    EQEQ("=="),
    LTEQ("<="),
    LTEQGT("<=>"),
    LTGT("<>"),
    GTEQ(">="),
    BANGEQ("!="),
    BANGGT("!>"),
    BANGLT("!<"),
    AMPAMP("&&"),
    BARBAR("||"),
    PLUS("+"),
    SUB("-"),
    STAR("*"),
    SLASH("/"),
    AMP("&"),
    BAR("|"),
    CARET("^"),
    PERCENT("%"),
    LTLT("<<"),
    GTGT(">>"),
    MONKEYS_AT("@");

    public final String name;

    DSQLToken() {
        this(null);
    }

    DSQLToken(String name) {
        this.name = name;
    }
}
