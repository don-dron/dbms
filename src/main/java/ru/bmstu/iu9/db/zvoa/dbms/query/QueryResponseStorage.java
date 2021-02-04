package ru.bmstu.iu9.db.zvoa.dbms.query;

import java.util.concurrent.BlockingQueue;

import ru.bmstu.iu9.db.zvoa.dbms.modules.DbStorage;
import ru.bmstu.iu9.db.zvoa.dbms.modules.Query;

/**
 * The type Query response storage.
 *
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public class QueryResponseStorage extends DbStorage<Query> {
    /**
     * Instantiates a new Query response storage.
     *
     * @param inputBuffer  the input buffer
     * @param outputBuffer the output buffer
     */
    public QueryResponseStorage(BlockingQueue inputBuffer, BlockingQueue outputBuffer) {
        super(inputBuffer, outputBuffer);
    }
}
