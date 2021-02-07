package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.storage.driver.lsm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.storage.driver.shared.Constants;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.storage.driver.shared.KVItem;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;

public class LSMFlusher extends Thread {

    private final Logger logger = LoggerFactory.getLogger(LSMFlusher.class);

    private static final long CACHE_FLUSH_FREQUENCY = 10000;
    private static final int MIN_FLUSH_SIZE = 200;
    private final LSMCache lsmCache;
    private final Path lsmFileDir;
    private final LSMLog lsmLog;

    private boolean shutDown = false;

    public LSMFlusher(LSMCache lsmCache, Path lsmFileDir, LSMLog lsmLog) {

        this.lsmCache = lsmCache;
        this.lsmFileDir = lsmFileDir;
        this.lsmLog = lsmLog;
    }

    @Override
    public void run() {
        while (!shutDown) {
            if (lsmCache.size() <= MIN_FLUSH_SIZE) {
                Thread.onSpinWait();
                continue;
            }
            TreeMap<String, KVItem> snapshot = lsmCache.getSnapshot();
            try {
                LSMFile lsmFile = new LSMFile(lsmFileDir);
                for (Map.Entry<String, KVItem> e : snapshot.entrySet()) {
                    lsmFile.append(e.getValue());
                }
                lsmFile.close();
                lsmLog.append(new KVItem(Constants.FLUSH_MESSAGE, "", 0));
                logger.debug("Flush success");
            } catch (IOException e) {
                logger.warn("Failed to flush cache", e);
            }
        }
    }
}
