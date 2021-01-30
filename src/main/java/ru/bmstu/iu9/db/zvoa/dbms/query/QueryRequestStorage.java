package ru.bmstu.iu9.db.zvoa.dbms.query;

import java.util.concurrent.BlockingQueue;

import ru.bmstu.iu9.db.zvoa.dbms.modules.DbStorage;
import ru.bmstu.iu9.db.zvoa.dbms.modules.Query;

public class QueryRequestStorage extends DbStorage<Query> {
    public QueryRequestStorage(BlockingQueue inputBuffer, BlockingQueue outputBuffer) {
        super(inputBuffer, outputBuffer);
    }
}
