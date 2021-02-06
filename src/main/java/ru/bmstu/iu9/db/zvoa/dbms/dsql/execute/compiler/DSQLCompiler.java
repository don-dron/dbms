package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.compiler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.compiler.lexer.DSQLLexer;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.compiler.optimizer.DSQLOptimizer;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.compiler.parser.DSQLParser;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.compiler.semanter.DSQLSemanter;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.compiler.translator.DSQLTranslator;
import ru.bmstu.iu9.db.zvoa.dbms.execute.CompilationError;
import ru.bmstu.iu9.db.zvoa.dbms.execute.IProgram;
import ru.bmstu.iu9.db.zvoa.dbms.execute.compiler.ICompiler;
import ru.bmstu.iu9.db.zvoa.dbms.execute.compiler.lexer.ILexer;
import ru.bmstu.iu9.db.zvoa.dbms.execute.compiler.lexer.IToken;
import ru.bmstu.iu9.db.zvoa.dbms.execute.compiler.lexer.LexerError;
import ru.bmstu.iu9.db.zvoa.dbms.execute.compiler.optimizer.IOptimizer;
import ru.bmstu.iu9.db.zvoa.dbms.execute.compiler.parser.ASTNode;
import ru.bmstu.iu9.db.zvoa.dbms.execute.compiler.parser.IParser;
import ru.bmstu.iu9.db.zvoa.dbms.execute.compiler.parser.ParserError;
import ru.bmstu.iu9.db.zvoa.dbms.execute.compiler.semanter.ISemanter;
import ru.bmstu.iu9.db.zvoa.dbms.execute.compiler.semanter.SemanticError;
import ru.bmstu.iu9.db.zvoa.dbms.execute.compiler.translator.ITranslator;

import java.util.stream.Stream;

public class DSQLCompiler implements ICompiler {
    private final Logger logger = LoggerFactory.getLogger(DSQLCompiler.class);

    private ILexer lexer = new DSQLLexer();
    private IParser parser = new DSQLParser();
    private ISemanter semanter = new DSQLSemanter();
    private ITranslator translator = new DSQLTranslator();
    private IOptimizer optimizer = new DSQLOptimizer();

    @Override
    public IProgram compile(String program) throws CompilationError {
        try {
            logger.debug("Start compilation program: " + program);
            Stream<IToken> tokenStream = lexer.lex(program);
            ASTNode programRoot = parser.parse(tokenStream);
            semanter.checkSemantic(programRoot);
            IProgram afterTranslator = translator.translate(programRoot);
            IProgram afterOptimizer = optimizer.optimize(afterTranslator);
            logger.debug("End compilation program without errors : " + program);
            return afterOptimizer;
        } catch (LexerError lexerError) {
            logger.debug("Compilation lexer error: " + lexerError.getMessage());
            throw new CompilationError(lexerError.getMessage());
        } catch (ParserError parserError) {
            logger.debug("Compilation parser error: " + parserError.getMessage());
            throw new CompilationError(parserError.getMessage());
        } catch (SemanticError semanticError) {
            logger.debug("Compilation semantic error: " + semanticError.getMessage());
            throw new CompilationError(semanticError.getMessage());
        } catch (Exception otherError) {
            logger.debug("Compilation internal error: " + otherError.getMessage());
            throw new CompilationError(otherError.getMessage());
        }
    }
}
