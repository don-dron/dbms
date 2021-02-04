package ru.bmstu.iu9.db.zvoa.dbms.query;

import ru.bmstu.iu9.db.zvoa.dbms.modules.DbStorage;
import ru.bmstu.iu9.db.zvoa.dbms.modules.Query;

import java.util.concurrent.BlockingQueue;

/**
 * The type Query request storage.
 *
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public class QueryRequestStorage extends DbStorage<Query> {
    /**
     * Instantiates a new Query request storage.
     *
     * @param inputBuffer  the input buffer
     * @param outputBuffer the output buffer
     */
    public QueryRequestStorage(BlockingQueue inputBuffer, BlockingQueue outputBuffer) {
        super(inputBuffer, outputBuffer);
    }
}
