package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.compiler.lexer;

import ru.bmstu.iu9.db.zvoa.dbms.execute.compiler.lexer.IToken;

import java.util.*;
import java.util.stream.Stream;

public class DSQLIdentifier {
    private boolean reservedFound = false;
    private boolean notReserved = false;
    private boolean intFound = false;
    private boolean floatFound = false;
    private boolean idFound = false;

    private int state = 0;
    private DSQLToken actualReserved = null;
    private int actualInt = 0;
    private double actualFloat = 0;
    private double floatDivider = 10;
    private List<Character> id = new ArrayList<>();
    private List<Character> buffer = new ArrayList<>();

    private boolean aheadReservedFound = false;
    private boolean aheadNotReserved = false;
    private boolean aheadIntFound = false;
    private boolean aheadFloatFound = false;
    private boolean aheadIdFound = false;

    private int aheadState = 0;
    private DSQLToken aheadReserved = null;
    private int aheadInt = 0;
    private double aheadFloat = 0;
    private double aheadFloatDivider = 10;
    private List<Character> aheadId = new ArrayList<>();
    private List<Character> aheadBuffer = new ArrayList<>();

    private Iterator<String> statesIterator;
    private List<DSQLToken> DSQLTokens = new ArrayList<>();
    private boolean firstRun = true;

    private String getStringRepresentation(List<Character> list) {
        StringBuilder builder = new StringBuilder(list.size());
        for (Character ch : list) {
            builder.append(ch);
        }
        return builder.toString();
    }

    private boolean addDSQLTokenIfWereAvailable() {
        boolean aheadReservedAccept = aheadReservedFound && !aheadNotReserved;
        boolean reservedAccept = reservedFound && !notReserved;
        boolean justOne = (floatFound ? 1 : 0) + (intFound ? 1 : 0) + (reservedAccept ? 1 : 0) + (idFound ? 1 : 0) == 1;
        justOne &= !aheadFloatFound && !aheadIntFound && !aheadReservedAccept && !aheadIdFound;

        if (!aheadFloatFound && floatFound && justOne) {
            DSQLTokens.add(new DSQLToken(DSQLTokenType.number, Double.toString(actualFloat)));
            return true;
        } else if (!aheadIntFound && intFound && justOne) {
            DSQLTokens.add(new DSQLToken(DSQLTokenType.integer, Integer.toString(actualInt)));
            return true;
        } else if (reservedAccept && !aheadIdFound) {
            DSQLTokens.add(actualReserved);
            return true;
        } else if (!aheadIdFound && idFound && justOne) {
            DSQLTokens.add(new DSQLToken(DSQLTokenType.id, getStringRepresentation(buffer)));
            return true;
        }

        return false;
    }

    private void resetState() {
        // Actual state
        reservedFound = false;
        notReserved = false;
        intFound = false;
        floatFound = false;
        idFound = false;

        state = 0;
        actualReserved = null;
        actualInt = 0;
        actualFloat = 0;
        floatDivider = 10;

        // Ahead State
        aheadReservedFound = false;
        aheadNotReserved = false;
        aheadIntFound = false;
        aheadFloatFound = false;
        aheadIdFound = false;

        aheadState = 0;
        aheadReserved = null;
        aheadInt = 0;
        aheadFloat = 0;
        aheadFloatDivider = 10;
        firstRun = true;
        buffer = new ArrayList<>();
        aheadBuffer = new ArrayList<>();
    }

    private void tickActualState() {
        reservedFound = aheadReservedFound;
        notReserved = aheadNotReserved;
        intFound = aheadIntFound;
        floatFound = aheadFloatFound;
        idFound = aheadIdFound;

        state = aheadState;
        actualReserved = aheadReserved;
        actualInt = aheadInt;
        actualFloat = aheadFloat;
        floatDivider = aheadFloatDivider;
        buffer = new ArrayList<>(aheadBuffer);
    }

    public void tickWithChar(char symbol) {
        boolean DSQLTokenAdded = false;
        aheadBuffer.add(symbol);
        if (floatFound || intFound || firstRun) {
            updateNumber(symbol);
        }
        if (idFound || firstRun) {
            updateId(symbol);
        }
        if (!notReserved || firstRun) {
            updateReservedWord(symbol);
        }
        DSQLTokenAdded = addDSQLTokenIfWereAvailable();
        if (DSQLTokenAdded) {
            resetState();
            if (!(symbol == ' ' || symbol == '\t' || symbol == '\r' || symbol == '\n')) {
                tickWithChar(symbol);
            }
        } else {
            tickActualState();
            firstRun = false;
        }
    }

    public Stream<IToken> identifyDSQLTokens(String input) {
        char[] word = input.toLowerCase().toCharArray();

        for (char character : word) {
            tickWithChar(character);
        }
        DSQLTokens.add(new DSQLToken(DSQLTokenType.EOF, null));
        return DSQLTokens.stream().map(dsqlToken -> (IToken) dsqlToken);
    }

    public void updateNumber(Character symbol) {
        if (symbol == '.') {
            aheadFloatFound = !floatFound;
            aheadIntFound = false;
            aheadFloat = actualInt;
        }
        if (Character.isDigit(symbol)) {
            if (!floatFound)
                intFound = true;

            if (intFound || firstRun) {
                aheadInt = actualInt * 10 + Character.getNumericValue(symbol);
                aheadIntFound = true;
            } else if (floatFound || firstRun) {
                aheadFloatDivider *= 10;
                aheadFloat += Character.getNumericValue(symbol) / floatDivider;
                aheadFloatFound = true;
            }
        } else if (symbol != '.') {
            aheadFloatFound = false;
            aheadIntFound = false;
        }
    }

    public void updateId(Character symbol) {
        // First char of an id must be a letter
        if (buffer.size() == 0) {
            aheadIdFound = Character.isLetter(symbol);
        } else {
            aheadIdFound = Character.isLetter(symbol) || Character.isDigit(symbol) || symbol == '_';
        }
    }

    private void createReservedStatesQueue(String remReservedWord) {
        LinkedList ll = new LinkedList(Arrays.asList(remReservedWord.split("")));
        statesIterator = ll.iterator();
    }

    private void updateReservedWord(Character symbol) {
        switch (state) {
            case 0:
                switch (symbol) {
                    case '"':
                        // Add remanining string from DSQLToken OPEN_PARENTHESIS
                        aheadReserved = new DSQLToken(DSQLTokenType.QUOTE, null);
                        aheadReservedFound = true;
                        break;
                    case '`':
                        // Add remanining string from DSQLToken OPEN_PARENTHESIS
                        aheadReserved = new DSQLToken(DSQLTokenType.QUOTE, null);
                        aheadReservedFound = true;
                        break;
                    case ')':
                        // Add remanining string from DSQLToken OPEN_PARENTHESIS
                        aheadReserved = new DSQLToken(DSQLTokenType.CLOSE_PARENTHESIS, null);
                        aheadReservedFound = true;
                        break;
                    case '(':
                        // Add remanining string from DSQLToken CLOSE_PARENTHESIS
                        aheadReserved = new DSQLToken(DSQLTokenType.OPEN_PARENTHESIS, null);
                        aheadReservedFound = true;
                        break;
                    case '*':
                        // Add remanining string from DSQLToken STAR
                        aheadReserved = new DSQLToken(DSQLTokenType.STAR, null);
                        aheadReservedFound = true;
                        break;
                    case ',':
                        // Add remanining string from DSQLToken COMMA
                        aheadReserved = new DSQLToken(DSQLTokenType.COMMA, null);
                        aheadReservedFound = true;
                        break;
                    case ';':
                        // Add remanining string from DSQLToken END_STATEMENT
                        aheadReserved = new DSQLToken(DSQLTokenType.END_STATEMENT, null);
                        aheadReservedFound = true;
                        break;
                    case '=':
                        // Add remanining string from DSQLToken OPERATOR
                        aheadReserved = new DSQLToken(DSQLTokenType.OPERATOR, null);
                        aheadReservedFound = true;
                        break;
                    case '<':
                        // Add remanining string from DSQLToken OPERATOR
                        aheadReserved = new DSQLToken(DSQLTokenType.OPERATOR, null);
                        aheadReservedFound = true;
                        aheadState = 8;
                        break;
                    case '>':
                        // Add remanining string from DSQLToken OPERATOR
                        aheadReserved = new DSQLToken(DSQLTokenType.OPERATOR, null);
                        aheadReservedFound = true;
                        aheadState = 9;
                        break;
                    case 'a':
                        aheadState = 1;
                        break;
                    case 'c':
                        aheadState = 10;
                        break;
                    case 'd':
                        aheadState = 2;
                        break;
                    case 'f':
                        aheadState = 6;
                        break;
                    case 'i':
                        aheadState = 4;
                        break;
                    case 'k':
                        // Add remanining string from DSQLToken KEY
                        aheadReserved = new DSQLToken(DSQLTokenType.KEY, null);
                        createReservedStatesQueue("ey");
                        aheadReservedFound = false;
                        aheadState = 16;
                        break;
                    case 'm':
                        // Add remanining string from DSQLToken MODIFY
                        aheadReserved = new DSQLToken(DSQLTokenType.MODIFY, null);
                        createReservedStatesQueue("odify");
                        aheadReservedFound = false;
                        aheadState = 16;
                        break;
                    case 'n':
                        aheadState = 5;
                        break;
                    case 'p':
                        // Add remanining string from DSQLToken PRIMARY
                        aheadReserved = new DSQLToken(DSQLTokenType.PRIMARY, null);
                        createReservedStatesQueue("rimary");
                        aheadReservedFound = false;
                        aheadState = 16;
                        break;
                    case 's':
                        // Add remanining string from DSQLToken SELECT
                        aheadReserved = new DSQLToken(DSQLTokenType.SELECT, null);
                        createReservedStatesQueue("elect");
                        aheadReservedFound = false;
                        aheadState = 16;
                        break;
                    case 'r':
                        aheadState = 7;
                        break;
                    case 'u':
                        // Add remanining string from DSQLToken USE
                        aheadReserved = new DSQLToken(DSQLTokenType.USE, null);
                        createReservedStatesQueue("se");
                        aheadReservedFound = false;
                        aheadState = 16;
                        break;
                    case 't':
                        aheadState = 3;
                        break;
                    case 'w':
                        // Add remanining string from DSQLToken WHERE
                        aheadReserved = new DSQLToken(DSQLTokenType.WHERE, null);
                        createReservedStatesQueue("here");
                        aheadReservedFound = false;
                        aheadState = 16;
                        break;
                    case 'v':
                        aheadState = 11;
                        break;
                    default:
                        aheadState = 17;
                        aheadNotReserved = true;
                }
                break;
            case 1:
                switch (symbol) {
                    case 'u':
                        // Add remanining string from DSQLToken AUTO_INCREMENT
                        aheadReserved = new DSQLToken(DSQLTokenType.AUTO_INCREMENT, null);
                        createReservedStatesQueue("to_increment");
                        aheadReservedFound = false;
                        aheadState = 16;
                        break;
                    case 'd':
                        // Add remanining string from DSQLToken ADD
                        aheadReserved = new DSQLToken(DSQLTokenType.ADD, null);
                        createReservedStatesQueue("d");
                        aheadReservedFound = false;
                        aheadState = 16;
                        break;
                    case 'l':
                        // Add remanining string from DSQLToken ALTER
                        aheadReserved = new DSQLToken(DSQLTokenType.ALTER, null);
                        createReservedStatesQueue("ter");
                        aheadReservedFound = false;
                        aheadState = 16;
                        break;
                    default:
                        aheadState = 17;
                        aheadNotReserved = true;
                }
                break;
            case 2:
                switch (symbol) {
                    case 'a':
                        // Add remanining string from DSQLToken DATABASE
                        aheadReserved = new DSQLToken(DSQLTokenType.DATABASE, null);
                        createReservedStatesQueue("tabase");
                        aheadReservedFound = false;
                        aheadState = 16;
                        break;
                    case 'r':
                        // Add remanining string from DSQLToken DROP
                        aheadReserved = new DSQLToken(DSQLTokenType.DROP, null);
                        createReservedStatesQueue("op");
                        aheadReservedFound = false;
                        aheadState = 16;
                        break;
                    case 'e':
                        // Add remanining string from DSQLToken DELETE
                        aheadReserved = new DSQLToken(DSQLTokenType.DELETE, null);
                        createReservedStatesQueue("lete");
                        aheadReservedFound = false;
                        aheadState = 16;
                        break;
                    default:
                        aheadState = 17;
                        aheadNotReserved = true;
                }
                break;
            case 3:
                switch (symbol) {
                    case 'a':
                        // Add remanining string from DSQLToken TABLE
                        aheadReserved = new DSQLToken(DSQLTokenType.TABLE, null);
                        createReservedStatesQueue("ble");
                        aheadReservedFound = false;
                        aheadState = 16;
                        break;
                    case 'r':
                        // Add remanining string from DSQLToken TRUNCATE
                        aheadReserved = new DSQLToken(DSQLTokenType.TRUNCATE, null);
                        createReservedStatesQueue("uncate");
                        aheadReservedFound = false;
                        aheadState = 16;
                        break;
                    default:
                        aheadState = 17;
                        aheadNotReserved = true;
                }
                break;
            case 4:
                switch (symbol) {
                    case 'n':
                        aheadState = 12;
                        break;
                    default:
                        aheadState = 17;
                        aheadNotReserved = true;
                }
                break;
            case 5:
                switch (symbol) {
                    case 'u':
                        aheadState = 13;
                        break;
                    case 'o':
                        // Add remanining string from DSQLToken NOT
                        aheadReserved = new DSQLToken(DSQLTokenType.NOT, null);
                        createReservedStatesQueue("t");
                        aheadReservedFound = false;
                        aheadState = 16;
                        break;
                    default:
                        aheadState = 17;
                        aheadNotReserved = true;
                }
                break;
            case 6:
                switch (symbol) {
                    case 'r':
                        // Add remanining string from DSQLToken FROM
                        aheadReserved = new DSQLToken(DSQLTokenType.FROM, null);
                        createReservedStatesQueue("om");
                        aheadReservedFound = false;
                        aheadState = 16;
                        break;
                    case 'o':
                        // Add remanining string from DSQLToken FOREIGN
                        aheadReserved = new DSQLToken(DSQLTokenType.FOREIGN, null);
                        createReservedStatesQueue("reign");
                        aheadReservedFound = false;
                        aheadState = 16;
                        break;
                    default:
                        aheadState = 17;
                        aheadNotReserved = true;
                }
                break;
            case 7:
                switch (symbol) {
                    case 'e':
                        aheadState = 14;
                        break;
                    default:
                        aheadState = 17;
                        aheadNotReserved = true;
                }
                break;
            case 8:
                switch (symbol) {
                    case '=':
                        // Add remanining string from DSQLToken OPERATOR
                        aheadReserved = new DSQLToken(DSQLTokenType.OPERATOR, null);
                        createReservedStatesQueue("=");
                        aheadReservedFound = false;
                        aheadState = 16;
                        break;
                    case '>':
                        // Add remanining string from DSQLToken OPERATOR
                        aheadReserved = new DSQLToken(DSQLTokenType.OPERATOR, null);
                        createReservedStatesQueue(">");
                        aheadReservedFound = false;
                        aheadState = 16;
                        break;
                    default:
                        aheadState = 17;
                        aheadNotReserved = true;
                }
                break;
            case 9:
                switch (symbol) {
                    case '=':
                        // Add remanining string from DSQLToken OPERATOR
                        aheadReserved = new DSQLToken(DSQLTokenType.OPERATOR, null);
                        createReservedStatesQueue("=");
                        aheadReservedFound = false;
                        aheadState = 16;
                        break;
                    default:
                        aheadState = 17;
                        aheadNotReserved = true;
                }
                break;
            case 10:
                switch (symbol) {
                    case 'h':
                        // Add remanining string from DSQLToken CHAR
                        aheadReserved = new DSQLToken(DSQLTokenType.CHAR, null);
                        createReservedStatesQueue("ar");
                        aheadReservedFound = false;
                        aheadState = 16;
                        break;
                    case 'r':
                        // Add remanining string from DSQLToken CREATE
                        aheadReserved = new DSQLToken(DSQLTokenType.CREATE, null);
                        createReservedStatesQueue("eate");
                        aheadReservedFound = false;
                        aheadState = 16;
                        break;
                    default:
                        aheadState = 17;
                        aheadNotReserved = true;
                }
                break;
            case 11:
                switch (symbol) {
                    case 'a':
                        aheadState = 15;
                        break;
                    default:
                        aheadState = 17;
                        aheadNotReserved = true;
                }
                break;
            case 12:
                switch (symbol) {
                    case 's':
                        // Add remanining string from DSQLToken INSERT
                        aheadReserved = new DSQLToken(DSQLTokenType.INSERT, null);
                        createReservedStatesQueue("ert");
                        aheadReservedFound = false;
                        aheadState = 16;
                        break;
                    case 't':
                        // Add remanining string from DSQLToken INTO
                        aheadReserved = new DSQLToken(DSQLTokenType.INTO, null);
                        createReservedStatesQueue("o");
                        aheadReservedFound = false;
                        aheadState = 16;
                        break;
                    default:
                        aheadState = 17;
                        aheadNotReserved = true;
                }
                break;
            case 13:
                switch (symbol) {
                    case 'm':
                        // Add remanining string from DSQLToken NUMERIC
                        aheadReserved = new DSQLToken(DSQLTokenType.NUMERIC, null);
                        createReservedStatesQueue("eric");
                        aheadReservedFound = false;
                        aheadState = 16;
                        break;
                    case 'l':
                        // Add remanining string from DSQLToken NULL
                        aheadReserved = new DSQLToken(DSQLTokenType.NULL, null);
                        createReservedStatesQueue("l");
                        aheadReservedFound = false;
                        aheadState = 16;
                        break;
                    default:
                        aheadState = 17;
                        aheadNotReserved = true;
                }
                break;
            case 14:
                switch (symbol) {
                    case 'f':
                        // Add remanining string from DSQLToken REFERENCES
                        aheadReserved = new DSQLToken(DSQLTokenType.REFERENCES, null);
                        createReservedStatesQueue("erences");
                        aheadReservedFound = false;
                        aheadState = 16;
                        break;
                    case 'n':
                        // Add remanining string from DSQLToken RENAME
                        aheadReserved = new DSQLToken(DSQLTokenType.RENAME, null);
                        createReservedStatesQueue("ame");
                        aheadReservedFound = false;
                        aheadState = 16;
                        break;
                    default:
                        aheadState = 17;
                        aheadNotReserved = true;
                }
                break;
            case 15:
                switch (symbol) {
                    case 'r':
                        // Add remanining string from DSQLToken VARCHAR
                        aheadReserved = new DSQLToken(DSQLTokenType.VARCHAR, null);
                        createReservedStatesQueue("char");
                        aheadReservedFound = false;
                        aheadState = 16;
                        break;
                    case 'l':
                        // Add remanining string from DSQLToken VALUES
                        aheadReserved = new DSQLToken(DSQLTokenType.VALUES, null);
                        createReservedStatesQueue("ues");
                        aheadReservedFound = false;
                        aheadState = 16;
                        break;
                    default:
                        aheadState = 17;
                        aheadNotReserved = true;
                }
                break;
            // State of queue check
            case 16:
                if (statesIterator.hasNext()) {
                    String next = statesIterator.next();
                    // If actual value if null get the next 
                    if (next.length() == 0) next = statesIterator.next();

                    char nextChar = next.charAt(0);
                    char inputChar = symbol;
                    // is the chars dont match
                    if (nextChar != inputChar) {
                        aheadReservedFound = false;
                        aheadNotReserved = true;
                        aheadState = 17;
                        return;
                    }
                    // If reach the end of the queue
                    if (!statesIterator.hasNext()) {
                        aheadReservedFound = true;
                        aheadState = 17;
                        return;
                    }
                }
                break;
            // Final aheadState of nonacceptance
            case 17:
            default:
                aheadReservedFound = false;
                aheadNotReserved = true;
        }
    }
}
