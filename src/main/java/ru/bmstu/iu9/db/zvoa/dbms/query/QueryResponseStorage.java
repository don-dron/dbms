package ru.bmstu.iu9.db.zvoa.dbms.query;

import java.util.concurrent.BlockingQueue;

import ru.bmstu.iu9.db.zvoa.dbms.modules.DbStorage;
import ru.bmstu.iu9.db.zvoa.dbms.modules.Query;

public class QueryResponseStorage extends DbStorage<Query> {
    public QueryResponseStorage(BlockingQueue inputBuffer, BlockingQueue outputBuffer) {
        super(inputBuffer, outputBuffer);
    }
}
