package ru.bmstu.iu9.db.zvoa.dbms.main;

import ru.bmstu.iu9.db.zvoa.dbms.DBMS;
import ru.bmstu.iu9.db.zvoa.dbms.utils.DBMSUtils;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

/**
 * The type Main.
 *
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public class Main {
    /**
     * The entry point of application.
     *
     * @param args the input arguments
     * @throws Exception the exception
     */
    public static void main(String[] args) throws Exception {
        DBMS dbms = DBMSUtils.createDefaultDBMS();
        dbms.init();
        Thread dbmsThread = new Thread(dbms);
        dbmsThread.start();

        while (true) {
            break;
        }
        dbmsThread.join();
    }
}
