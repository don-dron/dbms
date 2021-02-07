package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.engine.statement.select;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.engine.DSQLEngine;
import ru.bmstu.iu9.db.zvoa.dbms.execute.RuntimeError;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorage;

import java.util.List;

public class PlainSelectEngine extends DSQLEngine<PlainSelect> {
    public PlainSelectEngine(DataStorage dataStorage) {
        super(dataStorage);
    }

    public void execute(PlainSelect plainSelect) throws RuntimeError {

        List<SelectItem> selectItems = plainSelect.getSelectItems();
        FromItem fromItem = plainSelect.getFromItem();
        Expression where = plainSelect.getWhere();

        System.out.println(plainSelect);
    }
}
